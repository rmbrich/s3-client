const { PassThrough } = require('stream')
const s3 = require('@aws-sdk/client-s3')
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner')
const { Upload } = require('@aws-sdk/lib-storage')
const mime = require('mime-types')

const filenameRegex = /(?:.+\/)*(.+)$/
const safeKeyNameRegex = /[^a-zA-Z0-9_\-\ \.]/g

module.exports = class S3Client {
   #client

   constructor() {
      this.#client = new s3.S3Client({})
   }

   async getReadStream(bucket, key) {
      console.info('S3Client getReadStream', {bucket, key})

      try {
         const result = await this.#client.send(new s3.GetObjectCommand({
            Bucket: bucket,
            Key: key
         }))   
         return result.Body
      } catch (e) {
         throw e
      }
   }

   async getWriteStreamToS3(bucket, key, postProccessors = [], tagging) {      
      console.info('S3Client getWriteStream', {bucket, key, tagging})
      const passStream = new PassThrough({})
   
      try {
         let uploadParams = {
            Bucket: bucket, 
            Key: key,
            ContentType: mime.lookup(key) || 'application/octet-stream',
            Body: passStream
         }
         if (tagging) uploadParams.Tagging = tagging
         const upload = new Upload({
            client: this.#client,
            params: uploadParams
         })
         const uploadPromise = upload.done() // start the upload
         postProccessors.push(async () => {
            // after the pipes finish, wait for upload to finish         
            return uploadPromise
         })
      } catch (e) {
         throw (e)
      }
   
      return passStream
   }

   async getPresignedGetURL(bucket, key) {
      console.info('S3Client getPresignedGetURL', {bucket, key})

      try {
         const signedUrl = await getSignedUrl(this.#client, new s3.GetObjectCommand({
            Bucket: bucket,
            Key: key
         }), {
            expiresIn: 300
         })

         return {
            fileName: filenameRegex.exec(key)[1].replace(safeKeyNameRegex, '_'),
            downloadUrl: signedUrl
         }
      } catch (e) {
         throw e
      }
   }

   async getPresignedPutURL(bucket, key, tagging) {
      console.info('S3Client getPresignedPutURL', {bucket, key, tagging})
      
      try {
         let cmd = {
            Bucket: bucket,
            Key: key
         }
         if (tagging) cmd.Tagging = tagging
         const signedUrl = await getSignedUrl(this.#client, new s3.PutObjectCommand(cmd), {
            expiresIn: 86400,
            unhoistableHeaders: new Set(['x-amz-tagging'])
         })

         return {
            uploadUrl: signedUrl
         }
      } catch (e) {
         throw e
      }
   }

   async putObjectFromString(bucket, key, body = '') {
      console.info('S3Client pubObjectFromString', {bucket, key})

      try {
         let params = {
            Bucket: bucket,
            Key: key,
            ContentType: mime.lookup(key) || 'application/octet-stream',
            Body: Buffer.from(body)
         }
         await this.#client.send(new s3.PutObjectCommand(params))
         console.info(`Completed`)
      } catch (e) {
         throw e
      }
   }

   async deleteObjects(bucket, keys) {
      console.info('S3Client deleteObjects', {bucket, keys})

      try {
         let keyChunks = chunkArray(keys, 1000)
         let chunk = keyChunks.pop()
      
         while (Array.isArray(chunk) && chunk.length > 0) {
            try {
               await this.#client.send(new s3.DeleteObjectsCommand({
                  Bucket: bucket,
                  Delete: {
                     Objects: chunk.map((key) => {
                        return {
                           Key: key
                        }
                     })
                  }
               }))
      
               chunk = keyChunks.pop()
            } catch (e) {
               throw e
            }
         }
      
         console.info(`Deleted ${keys.length} objects`)
         return {
            deleted: keys.length
         }
      } catch (e) {
         throw e
      }
   }

   async listObjects(bucket, prefix, search, limit) {
      console.info('S3Client listObjects', {bucket, prefix, search, limit})

      let params = {
         Bucket: bucket,
         Prefix: prefix
      }
      let truncated = true
      let result = []
   
      while (truncated) {
         try {
            const response = await this.#client.send(new s3.ListObjectsV2Command(params))
            let filtered = response.Contents.reduce((out, obj) => {
               if (obj.Key.endsWith('/') && obj.Size == 0) return out //this is a folder
               let mapped = {
                  name: /.*\/(.*)/.exec(obj.Key)[1],
                  bucket: bucket,
                  key: obj.Key,
                  modified: obj.LastModified,
                  size: obj.Size
               }
               if (!search || search == '' || (search && mapped.key.includes(search))) out.push(mapped)
               return out
            }, [])
            result.push(...filtered)
   
            truncated = response.IsTruncated
            if (truncated) {
               params.ContinuationToken = response.NextContinuationToken
            }
         } catch (e) {
            throw e
         }
      }
   
      return limit ? result.slice(0, limit) : result
   }

   async copyObjects(bucket, sourceKeys, destinationPrefix, tagging) {
      console.info('S3Client copyObjects', {bucket, sourceKeys, destinationPrefix})
   
      let copyQueue = []
      try {
         for (key of sourceKeys) {
            let cmd = {
               CopySource: `${bucket}/${key}`,
               Bucket: bucket,
               Key: `${destinationPrefix}/${/.*\/(.*)/.exec(key)[1]}`
            }
            if (tagging) {
               cmd.TaggingDirective = 'REPLACE'
               cmd.Tagging = tagging
            } else {
               cmd.TaggingDirective = 'COPY'
            }
            copyQueue.push(this.#client.send(new s3.CopyObjectCommand(cmd)))
   
            while (copyQueue.length > 1000) {
               await copyQueue.shift()
            }
         }
         await Promise.all(copyQueue)
   
         return {
            filesCopied: sourceKeys.length
         }
      } catch(e) {
         throw e
      }
   }

   async execPostProcessors(processors) {
      return Promise.all(
         _.map(processors, function(process) {
            if (typeof process == 'function') {
               console.debug('S3Client executing post processor')
               return process()
            }
         })
      )
   }

   async createMultipartUpload(bucket, key) {
      console.info('S3Client multipart createMultipartUpload', {bucket, key})
   
      try {
         const multipartUpload = await this.#client.send(new s3.CreateMultipartUploadCommand({
            Bucket: bucket,
            Key: key
         }))
      
         return {
            uploadId: multipartUpload.UploadId,
            key: multipartUpload.Key,
         }
      } catch (e) {
         throw e
      }
   }
   
   async listParts(bucket, key, uploadId) {
      console.info('S3Client multipart listParts', {bucket, key, uploadId})
   
      try {
         const part = await this.#client.send(new s3.ListPartsCommand({
            Bucket: bucket,
            Key: key,
            UploadId: uploadId
         }))
   
         return part.Parts || []
      } catch (e) {
         throw e
      }
   }
   
   async signPart(bucket, key, uploadId, partNumber) {
      console.info('S3Client multipart signPart', {bucket, key, uploadId, partNumber})
   
      try {
         const signedUrl = await getSignedUrl(this.#client, new s3.UploadPartCommand({
            Bucket: bucket,
            Key: key,
            UploadId: uploadId,
            PartNumber: partNumber
         }), {
            expiresIn: 86400
         })
   
         return {
            url: signedUrl
         }
      } catch (e) {
         throw e
      }
   }
   
   async abortMultipartUpload(bucket, key, uploadId) {
      console.info('S3Client multipart abortMultipartUpload', {bucket, key, uploadId})
   
      try {
         let result = await this.#client.send(new s3.AbortMultipartUploadCommand({
            Bucket: bucket,
            Key: key,
            UploadId: uploadId
         }))
   
         return result
      } catch (e) {
         throw e
      }
   }
   
   async completeMultipartUpload(bucket, key, uploadId, parts = []) {
      console.info('S3Client multipart completeMultipartUpload', {bucket, key, uploadId, parts})
   
      try {
         let result = await this.#client.send(new s3.CompleteMultipartUploadCommand({
            Bucket: bucket,
            Key: key,
            UploadId: uploadId,
            MultipartUpload: {               
               Parts: parts
            }
         }))
         
         return result
      } catch (e) {
         throw e
      }
   }
}

function chunkArray(arr, chunkSize) {
   const res = [];
   for (let i = 0; i < arr.length; i += chunkSize) {
       const chunk = arr.slice(i, i + chunkSize);
       res.push(chunk);
   }
   return res;
}