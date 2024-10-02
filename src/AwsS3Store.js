const path = require('path');
const fs = require('fs');
const unzipper = require('unzipper');

class AwsS3Store {
  /**
   * A class for storing authentication data of Whatsapp-web.js to AWS S3.
   * @example
   * For example usage see `example/index.js`.
   * @param {Object} options Specifies the params pattern.
   * @param {String} options.bucketName Specifies the S3 bucket name.
   * @param {String} options.remoteDataPath Specifies the remote path to save authentication files.
   * @param {Object} options.s3Client The S3Client instance after configuring the AWS SDK.
   * @param {Object} options.putObjectCommand  The PutObjectCommand class from `@aws-sdk/client-s3`.
   * @param {Object} options.headObjectCommand  The HeadObjectCommand class from `@aws-sdk/client-s3`.
   * @param {Object} options.getObjectCommand  The GetObjectCommand class from `@aws-sdk/client-s3`.
   * @param {Object} options.deleteObjectCommand  The DeleteObjectCommand class from `@aws-sdk/client-s3`.
   */
  constructor({ bucketName, remoteDataPath, s3Client, putObjectCommand, headObjectCommand, getObjectCommand, deleteObjectCommand } = {}) {
    if (!bucketName) throw new Error("A valid bucket name is required for AwsS3Store.");
    if (!remoteDataPath) throw new Error("A valid remote dir path is required for AwsS3Store.");
    if (!s3Client) throw new Error("A valid S3Client instance is required for AwsS3Store.");
    this.bucketName = bucketName;
    this.remoteDataPath = remoteDataPath;
    this.s3Client = s3Client;
    this.putObjectCommand = putObjectCommand;
    this.headObjectCommand = headObjectCommand;
    this.getObjectCommand = getObjectCommand;
    this.deleteObjectCommand = deleteObjectCommand;
    this.debugEnabled = process.env.STORE_DEBUG === 'true';
  }

  async sessionExists(options) {
    this.debugLog('[METHOD: sessionExists] Triggered.');

    const remoteFilePath = path.join(this.remoteDataPath, `${options.session}.zip`).replace(/\\/g, '/');
    const params = {
      Bucket: this.bucketName,
      Key: remoteFilePath
    };
    this.debugLog(`[METHOD: sessionExists] PARAMS='${JSON.stringify(params)}'.`);
    try {
      await this.s3Client.send(new this.headObjectCommand(params));
      this.debugLog(`[METHOD: sessionExists] File found. PATH='${remoteFilePath}'.`);
      return true;
    } catch (err) {
      if (err.name === 'NoSuchKey' || err.name === 'NotFound') {
        this.debugLog(`[METHOD: sessionExists] File not found. PATH='${remoteFilePath}'.`);
        return false;
      }
      this.debugLog(`[METHOD: sessionExists] Error: ${err.message}`);
      // throw err;
      return
    }
  }

  async save(options) {
    this.debugLog('[METHOD: save] Triggered.');
    const remoteFilePath = path.join(this.remoteDataPath, `${options.session}.zip`).replace(/\\/g, '/');
    options.remoteFilePath = remoteFilePath;
    console.log('[AWS S3] Remote file path', remoteFilePath)

    const fileStream = fs.createReadStream(`/tmp/${options.session}.zip`);
    const params = {
      Bucket: this.bucketName,
      Key: remoteFilePath,
      Body: fileStream
    };
    await this.s3Client.send(new this.putObjectCommand(params));
    this.debugLog(`[METHOD: save] File saved. PATH='${remoteFilePath}'.`);
  }

  async extract(options) {
    this.debugLog('[METHOD: extract] Triggered.');
  
    const remoteFilePath = path.join(this.remoteDataPath, `${options.session}.zip`).replace(/\\/g, '/');
    const params = {
      Bucket: this.bucketName,
      Key: remoteFilePath
    };
    const fileStream = fs.createWriteStream(options.path);
  
    try {
      const response = await this.s3Client.send(new this.getObjectCommand(params));
  
      if (!response.Body) {
        throw new Error('No body found in S3 response');
      }
  
      console.log('Justo antes del pipe');
  
      // Esperamos hasta que el archivo se descargue completamente
      await new Promise((resolve, reject) => {
        response.Body.pipe(fileStream)
          .on('error', (err) => {
            console.error('Error during file writing', err);
            reject(err);
          })
          .on('finish', () => {
            console.log('File successfully written');
            resolve();
          });
      });
  
      this.debugLog(`[METHOD: extract] File extracted. REMOTE_PATH='${remoteFilePath}', LOCAL_PATH='${options.path}'.`);
  
      // Ahora procesamos el archivo zip usando unzipper
      fs.createReadStream(options.path)
        .pipe(unzipper.Extract({ path: 'output_path' }))
        .on('error', (err) => {
          console.error('Error during unzip', err);
          throw err;
        })
        .on('close', () => {
          console.log('Unzip completed');
        });
  
    } catch (err) {
      console.error('Error during extract', err);
      throw err;
    }
  }

  async delete(options) {
    this.debugLog('[METHOD: delete] Triggered.');

    const remoteFilePath = path.join(this.remoteDataPath, `${options.session}.zip`).replace(/\\/g, '/');
    const params = {
      Bucket: this.bucketName,
      Key: remoteFilePath
    };
    try {
      await this.s3Client.send(new this.headObjectCommand(params));
      await this.s3Client.send(new this.deleteObjectCommand(params));
      this.debugLog(`[METHOD: delete] File deleted. PATH='${remoteFilePath}'.`);
    } catch (err) {
      if (err.name === 'NoSuchKey' || err.name === 'NotFound') {
        this.debugLog(`[METHOD: delete] File not found. PATH='${remoteFilePath}'.`);
        return;
      } 
      this.debugLog(`[METHOD: delete] Error: ${err.message}`);
      // throw err;
      return
    }
  }

  async #deletePrevious(options) {
    this.debugLog('[METHOD: #deletePrevious] Triggered.');

    const params = {
      Bucket: this.bucketName,
      Key: options.remoteFilePath
    };
    try {
      await this.s3Client.send(new this.headObjectCommand(params));
      await this.s3Client.send(new this.deleteObjectCommand(params));
      this.debugLog(`[METHOD: #deletePrevious] File deleted. PATH='${options.remoteFilePath}'.`);
    } catch (err) {
      if (err.name === 'NoSuchKey' || err.name === 'NotFound') {
        this.debugLog(`[METHOD: #deletePrevious] File not found. PATH='${options.remoteFilePath}'.`);
        return;
      }
      this.debugLog(`[METHOD: #deletePrevious] Error: ${err.message}`);
      // throw err;
      return
    }
  }

  debugLog(msg) {
    if (true) {
      const timestamp = new Date().toISOString();
      console.log(`${timestamp} [STORE_DEBUG] ${msg}`);
    }
  }
}

module.exports = AwsS3Store;
