/***
An abstraction of the file storage provider. One advantage of this architecture (separation of
 concerns and single responsibility principle) is that the video storage microservice can be easily
 swapped out and be replaced with an alternative.

IBM's Cloud Object Storage (COS) is S3 (Simple Storage Service) compatible and can, thus, be used
 with any S3-compatible tooling. The fundamental unit of object storage is called a "bucket".
***/
const express = require("express");
//https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-node
const cos = require("ibm-cos-sdk");
const stream = require('stream');
const winston = require('winston');

/******
Globals
******/
//Create a new express instance.
const app = express();
const SVC_NAME = process.env.SVC_NAME;
const APP_NAME_VER = process.env.APP_NAME_VER;
const BUCKET_NAME = process.env.BUCKET_NAME;
const AUTHENTICATION_TYPE = process.env.AUTHENTICATION_TYPE;
const API_KEY = process.env.API_KEY;
const ENDPOINT = process.env.ENDPOINT;
const ACCESS_KEY_ID = process.env.ACCESS_KEY_ID;
const SECRET_ACCESS_KEY = process.env.SECRET_ACCESS_KEY;
const SERVICE_INSTANCE_ID = process.env.SERVICE_INSTANCE_ID;
const REGION = process.env.REGION;
const PORT = process.env.PORT && parseInt(process.env.PORT) || 3000;
let READINESS_PROBE = false;
const CONFIG = AUTHENTICATION_TYPE.toLowerCase() === 'hmac' ?
                //For services that use HMAC credentials for authentication.
                {
                  apiVersion: 'latest',  //Use the latest possible version.
                  accessKeyId: ACCESS_KEY_ID,
                  secretAccessKey: SECRET_ACCESS_KEY,
                  region: REGION,
                  endpoint: ENDPOINT
                } :
                //For services that use IAM authentication.
                {
                  apiVersion: 'latest',
                  apiKeyId: API_KEY,
                  serviceInstanceId: SERVICE_INSTANCE_ID,
                  //signatureVersion: 'iam',
                  endpoint: ENDPOINT
                };
const client = new cos.S3(CONFIG);

/***
Resume Operation
----------------
The resume operation strategy intercepts unexpected errors and responds by allowing the process to
continue.
***/
process.on('uncaughtException',
err => {
  logger.error('Uncaught exception.', { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  logger.error(err.stack, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
})

/***
Abort and Restart
-----------------
***/
// process.on("uncaughtException",
// err => {
//   console.error("Uncaught exception:");
//   console.error(err && err.stack || err);
//   process.exit(1);
// })

//Winston requires at least one transport (location to save the log) to create a log.
const logConfiguration = {
  transports: [
    new winston.transports.Console()
  ],
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD hh:mm:ss.SSS'
    }),
    winston.format.json()
  ),
  exitOnError: false
}

//Create a logger and pass it the Winston configuration object.
const logger = winston.createLogger(logConfiguration);

/***
Unlike most other programming languages or runtime environments, Node.js doesn't have a built-in
special "main" function to designate the entry point of a program.

Accessing the main module
-------------------------
When a file is run directly from Node.js, require.main is set to its module. That means that it is
possible to determine whether a file has been run directly by testing require.main === module.
***/
if (require.main === module) {
  main()
  .then(() => {
    READINESS_PROBE = true;
    logger.info(`Microservice is listening on port ${PORT}!`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  })
  .catch(err => {
    logger.error('Microservice failed to start.', { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    logger.error(err.stack, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  });
}

function main() {
  //Throw an exception if any required environment variables are missing.
  if (!process.env.BUCKET_NAME) {
    throw new Error('Please specify the bucket name of an IBM COS account in the environment variable BUCKET_NAME.');
  } else if (!process.env.ENDPOINT) {
    throw new Error('Please specify the endpoint for the IBM COS account in the environment variable ENDPOINT.');
  } else if (!process.env.AUTHENTICATION_TYPE) {
    throw new Error('Please specify the type of authentication to use in the environment variable AUTHENTICATION_TYPE.');
  } else if (AUTHENTICATION_TYPE.toLowerCase() === 'hmac') {
    if (!process.env.REGION) {
      throw new Error('Please specify the region in the environment variable REGION.');
    } else if (!process.env.SECRET_ACCESS_KEY) {
      throw new Error('Please specify the HMAC secret access key in the environment variable SECRET_ACCESS_KEY.');
    } else if (!process.env.ACCESS_KEY_ID) {
      throw new Error('Please specify the HMAC access key id in the environment variable ACCESS_KEY_ID.');
    }
  } else if (AUTHENTICATION_TYPE.toLowerCase() === 'iam') {
    if (!process.env.API_KEY) {
      throw new Error('Please specify the API key of an IBM COS account in the environment variable API_KEY.');
    } else if (!process.env.SERVICE_INSTANCE_ID) {
      throw new Error('Please specify the service instance Id for the IBM COS account in the environment variable SERVICE_INSTANCE_ID.');
    }
  } else {
    throw new Error(`Unknown authentication type (${AUTHENTICATION_TYPE}).`);
  }
  //Display a message if any optional environment variables are missing.
  if (process.env.PORT === undefined) {
    logger.info(`The environment variable PORT for the HTTP server is missing; using port ${PORT}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  }
  //For debugging...
  //console.log(require('util').inspect(CONFIG));
  //console.log(require('util').inspect(client.config));
  //Notify when server has started.
  return new Promise(resolve => {
    app.listen(PORT,
    () => {
      resolve();  //HTTP server is listening, resolve the promise.
    });
  });
}

/****************
Route definitions
****************/
//Readiness probe.
app.get('/readiness',
(req, res) => {
  res.sendStatus(READINESS_PROBE === true ? 200 : 500);
});

//HTTP GET route to stream a video from COS.
app.get("/video",
(req, res) => {
  /***
  In the HTTP protocol, headers are case-insensitive; however, the Express framework converts
  everything to lower case. Unfortunately, for objects in JavaScript, their property names are
  case-sensitive.
  ***/
  const cid = req.headers['x-correlation-id'];
  const videoId = req.query.id;
  if (videoId !== undefined) {
    logger.info(`Retrieving video from bucket: ${BUCKET_NAME}, key: ${videoId}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
    const params = {
      Bucket: BUCKET_NAME,
      Key: videoId
    };
    client.getObject(params)
    .promise()
    .then(data => {
      logger.info(`Retrieved ${BUCKET_NAME}/${videoId} with size ${data.ContentLength}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      //Headers
      res.set("Content-Length", data.ContentLength)
         .set("Content-Type", data.ContentType);
      //Streaming the video...
      res.send(data.Body);
    })
    .catch(err => {
      if (err.code === "NoSuchKey") {
        logger.info(`${BUCKET_NAME}/${videoId} not found.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      } else {
        logger.error(`Error while retrieving video ${BUCKET_NAME}/${videoId} to stream.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
        logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
        logger.error(err.stack, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      }
      res.sendStatus(500);
    });
  } else {
    logger.info('An id term must be provided.', { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
    res.send({ error: "An 'id' term must be provided." });
  }
});

//HTTP POST route to upload a video to COS.
app.post('/upload',
(req, res) => {
  const videoId = req.headers.id;
  const mimeType = req.headers['content-type'];
  const contentLength = req.headers['content-length'];
  const cid = req.headers['x-correlation-id'];
  let passThrough = new stream.PassThrough();
  const params = {
    Bucket: BUCKET_NAME,
    Key: videoId,
    ContentType: mimeType,
    ContentLength: contentLength,
    Body: req.pipe(passThrough)
  };
  logger.info(`Uploading video to bucket: ${BUCKET_NAME}, key: ${videoId}, Content-Type: ${mimeType}, Content-Length: ${contentLength}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
  client.putObject(params)
  .promise()
  .then(data => {
    logger.info(`Uploaded the video ${videoId}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
    res.sendStatus(200);
  })
  .catch(err => {
    logger.error(`Upload to COS failed for video ${videoId}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
    logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
    res.sendStatus(500);
  });
});

/***
The 404 Route
-------------
In Express, 404 responses are not the result of an error, so the error-handler middleware will not
capture them. This behavior is because a 404 response simply indicates the absence of additional
work to do; in other words, Express has executed all middleware functions and routes, and found
that none of them responded. All you need to do is add a middleware function at the VERY BOTTOM of
the stack (below all other functions) to handle a 404 response.
***/
app.use((req, res, next) => {
  logger.error(`Unable to find the requested resource (${req.url})!`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  res.status(404).send(`<h1>Unable to find the requested resource (${req.url})!</h1>`);
});

function getListOfFiles(prefix) {
  return new Promise((resolve, reject) => {
    let params = {
      Bucket: BUCKET_NAME,
      MaxKeys: 1000,
      Prefix: prefix,
      Delimiter: prefix
    };
    let allKeys = [];
    listAllKeys();
    function listAllKeys() {
      CLIENT.listObjectsV2(params,
      (err, data) => {
        if (err) {
          console.log(err);
          reject(err)
        } else {
          data.Contents.forEach(content => {
            allKeys.push(content.Key);
          });
          //
          if (data.IsTruncated) {
            params.ContinuationToken = data.NextContinuationToken;
            listAllKeys();
          } else {
            resolve(allKeys);
          }
        }
      });
    }
  });
}


/***
//Retrieve file from COS.
function getFiles()
{
  return getListOfFiles('')
  .then(listOfKeys =>
  {
    let promises = [];
    for (let index = 0; index < listOfKeys.length; ++index)
    {
      promises.push(retrieveFileContent(listOfKeys[index]));
    }
    //
    function retrieveFileContent(key)
    {
      console.log(`Retrieving file from bucket: ${BUCKET_NAME}, key: ${key}`);
      const params =
      {
        Bucket: BUCKET_NAME,
        Key: key
      };
      return new Promise(resolve =>
      {
        CLIENT.getObject(params)
        .promise()
        .then(data =>
        {
          console.log(`Retrieved ${BUCKET_NAME}/${key} with size ${data.ContentLength}`);
          resolve([`${key}`, data]);
        })
        .catch(err =>
        {
          if (err.code === "NoSuchKey")
          {
            console.error(`${BUCKET_NAME}/${key} not found.`);
          }
          else
          {
            console.error(`Error occurred getting audio ${BUCKET_NAME}/${key} to stream.`);
            console.error(err.stack);
          }
        });
      });
    };
    //
    Promise.all(promises)
    .then(arr =>
    {
      for (let index = 0; index < arr.length; ++index)
      {
        const recognizeParams =
        {
          audio: arr[index][1].Body,
          //objectMode: true,
          contentType: arr[index][1].ContentType,
          //wordAlternativesThreshold: 0.9,
          model: 'en-US_BroadbandModel',
          //keywords: ['colorado', 'tornado', 'tornadoes'],
          //keywordsThreshold: 0.5,
          maxAlternatives: 1
        };
        speechToText.recognize(recognizeParams)
        .then(speechRecognitionResults =>
        {
          const buffer = speechRecognition(speechRecognitionResults.result);
          console.log(`Translated file ${BUCKET_NAME}/${arr[index][0]} to text.`);
          console.log(buffer);
        })
        .catch(err =>
        {
          console.log('error:', err);
        });
      }
    });
  });
}
***/
