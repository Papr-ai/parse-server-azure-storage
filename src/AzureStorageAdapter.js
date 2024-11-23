// AzureStorageAdapter
//
// Stores Parse files in Azure Blob Storage.

import { BlobServiceClient, PublicAccessType } from '@azure/storage-blob';
import requiredParameter from './RequiredParameter';

export class AzureStorageAdapter {
  // Creates an Azure Storage Client.
  constructor(
    accountName = requiredParameter('AzureStorageAdapter requires an account name'),
    container = requiredParameter('AzureStorageAdapter requires a container'),
    { accessKey = '',
      directAccess = false } = {}
  ) {
    this._accountName = accountName;
    this._accessKey = accessKey;
    this._container = container;
    this._directAccess = directAccess;

    // Init client
    const connectionString = `DefaultEndpointsProtocol=https;AccountName=${accountName};AccountKey=${accessKey};EndpointSuffix=core.windows.net`;
    this._client = BlobServiceClient.fromConnectionString(connectionString);
    this._containerClient = this._client.getContainerClient(container);
  }

  /**
   * For a given filename and data, store a file in Azure Blob Storage
   * @param  {string} filename
   * @param  {string} data
   * @return {Promise} Promise containing the Azure Blob Storage blob creation response
   */
  async createFile(filename, data) {
    try {
      // Create container if it doesn't exist with proper access level
      const options = this._directAccess ? { access: 'blob' } : {};
      await this._containerClient.createIfNotExists(options);

      // Upload the file
      const blockBlobClient = this._containerClient.getBlockBlobClient(filename);
      const buffer = Buffer.from(data);
      const uploadResponse = await blockBlobClient.upload(buffer, buffer.length);
      return uploadResponse;
    } catch (error) {
      throw new Error(`Error creating file: ${error.message}`);
    }
  }

  /**
   * Delete a file if found by filename
   * @param  {string} filename
   * @return {Promise} Promise that succeeds with the result from Azure Storage
   */
  async deleteFile(filename) {
    try {
      const blockBlobClient = this._containerClient.getBlockBlobClient(filename);
      const deleteResponse = await blockBlobClient.delete();
      return deleteResponse;
    } catch (error) {
      throw new Error(`Error deleting file: ${error.message}`);
    }
  }

  /**
   * Search for and return a file if found by filename
   * @param  {string} filename
   * @return {Promise<Buffer>} Promise that succeeds with the file data as a Buffer
   */
  async getFileData(filename) {
    try {
      const blockBlobClient = this._containerClient.getBlockBlobClient(filename);
      const downloadResponse = await blockBlobClient.download(0);
      
      return new Promise((resolve, reject) => {
        const chunks = [];
        downloadResponse.readableStreamBody.on('data', (chunk) => {
          chunks.push(chunk instanceof Buffer ? chunk : Buffer.from(chunk));
        });
        downloadResponse.readableStreamBody.on('end', () => {
          resolve(Buffer.concat(chunks));
        });
        downloadResponse.readableStreamBody.on('error', reject);
      });
    } catch (error) {
      throw new Error(`Error getting file data: ${error.message}`);
    }
  }

  /**
   * Generates and returns the location of a file stored in Azure Blob Storage
   * @param  {object} config
   * @param  {string} filename
   * @return {string} file's url
   */
  getFileLocation(config, filename) {
    if (this._directAccess) {
      return `https://${this._accountName}.blob.core.windows.net/${this._container}/${filename}`;
    }
    return (`${config.mount}/files/${config.applicationId}/${encodeURIComponent(filename)}`);
  }
}

export default AzureStorageAdapter;
