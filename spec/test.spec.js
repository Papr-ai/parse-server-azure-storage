'use strict';
require('dotenv').config();

const AzureStorageAdapter = require('../src/AzureStorageAdapter.js').default;

describe('Azure Storage Adapter', () => {
  let adapter;
  const testFileName = 'test-file.txt';
  const testFileContent = Buffer.from('Hello, Azure!');

  beforeEach(() => {
    if (process.env.AZURE_ACCOUNT_NAME && process.env.AZURE_CONTAINER && process.env.AZURE_ACCESS_KEY) {
      adapter = new AzureStorageAdapter(
        process.env.AZURE_ACCOUNT_NAME,
        process.env.AZURE_CONTAINER,
        { 
          accessKey: process.env.AZURE_ACCESS_KEY,
          directAccess: false 
        }
      );
    } else {
      console.warn('Azure credentials not found in environment variables. Skipping integration tests.');
    }
  });

  describe('Constructor', () => {
    it('should throw when not initialized properly', () => {
      expect(() => {
        new AzureStorageAdapter();
      }).toThrow('AzureStorageAdapter requires an account name');

      expect(() => {
        new AzureStorageAdapter('accountName');
      }).toThrow('AzureStorageAdapter requires a container');
    });

    it('should not throw when initialized properly', () => {
      expect(() => {
        new AzureStorageAdapter('accountName', 'container', {
          accessKey: Buffer.from('accessKey').toString('base64')
        });
      }).not.toThrow();
    });
  });

  // Only run these tests if Azure credentials are configured
  (process.env.AZURE_ACCOUNT_NAME ? describe : describe.skip)('File Operations', () => {
    it('should create and delete a file', async () => {
      const createResult = await adapter.createFile(testFileName, testFileContent);
      expect(createResult).toBeDefined();
      expect(createResult.requestId || createResult._response?.requestId).toBeDefined();

      const deleteResult = await adapter.deleteFile(testFileName);
      expect(deleteResult).toBeDefined();
    });

    it('should get file data', async () => {
      await adapter.createFile(testFileName, testFileContent);
      
      const data = await adapter.getFileData(testFileName);
      expect(Buffer.isBuffer(data)).toBe(true);
      expect(data.toString()).toBe(testFileContent.toString());
      
      await adapter.deleteFile(testFileName);
    });

    it('should generate correct file location', () => {
      const config = {
        mount: '/parse',
        applicationId: 'test-app'
      };
      
      const location = adapter.getFileLocation(config, testFileName);
      
      if (adapter._directAccess) {
        expect(location).toBe(
          `https://${process.env.AZURE_ACCOUNT_NAME}.blob.core.windows.net/${process.env.AZURE_CONTAINER}/${testFileName}`
        );
      } else {
        expect(location).toBe(
          `/parse/files/test-app/${encodeURIComponent(testFileName)}`
        );
      }
    });
  });
});
