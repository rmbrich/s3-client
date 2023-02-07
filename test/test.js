require('dotenv').config()
const S3Client = require('../src/s3')
const s3 = new S3Client()

const run1 = async () => {
   //let result = await s3.listObjects()

   console.log(result)
}
run1()