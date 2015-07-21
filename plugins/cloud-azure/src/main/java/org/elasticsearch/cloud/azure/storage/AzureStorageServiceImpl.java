/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cloud.azure.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryException;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage.*;

/**
 *
 */
public class AzureStorageServiceImpl extends AbstractLifecycleComponent<AzureStorageServiceImpl>
    implements AzureStorageService {

    private final String account;
    private final String key;
    private final String blob;

    private CloudBlobClient client;

    @Inject
    public AzureStorageServiceImpl(Settings settings) {
        super(settings);
        // We try to load storage API settings from `cloud.azure.`
        account = settings.get(ACCOUNT);
        key = settings.get(KEY);
        blob = "http://" + account + ".blob.core.windows.net/";

        try {
            if (account != null) {
                logger.trace("creating new Azure storage client using account [{}], key [{}], blob [{}]", account, key, blob);

                String storageConnectionString =
                        "DefaultEndpointsProtocol=http;"
                                + "AccountName="+ account +";"
                                + "AccountKey=" + key;

                // Retrieve storage account from connection-string.
                CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

                // Create the blob client.
                client = storageAccount.createCloudBlobClient();
            }
        } catch (Exception e) {
            // Can not start Azure Storage Client
            logger.error("can not start azure storage client: {}", e.getMessage());
        }
    }

    @Override
    public boolean doesContainerExist(String container) {
        try {
            CloudBlobContainer blob_container = client.getContainerReference(container);
            return blob_container.exists();
        } catch (Exception e) {
            logger.error("can not access container [{}]", container);
        }
        return false;
    }

    @Override
    public void removeContainer(String container) throws URISyntaxException, StorageException {
        CloudBlobContainer blob_container = client.getContainerReference(container);
        // TODO Should we set some timeout and retry options?
        /*
        BlobRequestOptions options = new BlobRequestOptions();
        options.setTimeoutIntervalInMs(1000);
        options.setRetryPolicyFactory(new RetryNoRetry());
        blob_container.deleteIfExists(options, null);
        */
        logger.trace("removing container [{}]", container);
        blob_container.deleteIfExists();
    }

    @Override
    public void createContainer(String container) throws URISyntaxException, StorageException {
        try {
            CloudBlobContainer blob_container = client.getContainerReference(container);
            logger.trace("creating container [{}]", container);
            blob_container.createIfNotExists();
        } catch (IllegalArgumentException e) {
            logger.trace("fails creating container [{}]", container, e.getMessage());
            throw new RepositoryException(container, e.getMessage());
        }
    }

    @Override
    public void deleteFiles(String container, String path) throws URISyntaxException, StorageException {
        logger.trace("delete files container [{}], path [{}]", container, path);

        // Container name must be lower case.
        CloudBlobContainer blob_container = client.getContainerReference(container);
        if (blob_container.exists()) {
            for (ListBlobItem blobItem : blob_container.listBlobs(path)) {
                logger.trace("removing blob [{}]", blobItem.getUri());
                deleteBlob(container, blobItem.getUri().toString());
            }
        }
    }

    @Override
    public boolean blobExists(String container, String blob) throws URISyntaxException, StorageException {
        // Container name must be lower case.
        CloudBlobContainer blob_container = client.getContainerReference(container);
        if (blob_container.exists()) {
            CloudBlockBlob azureBlob = blob_container.getBlockBlobReference(blob);
            return azureBlob.exists();
        }

        return false;
    }

    @Override
    public void deleteBlob(String container, String blob) throws URISyntaxException, StorageException {
        logger.trace("delete blob for container [{}], blob [{}]", container, blob);

        // Container name must be lower case.
        CloudBlobContainer blob_container = client.getContainerReference(container);
        if (blob_container.exists()) {
            logger.trace("container [{}]: blob [{}] found. removing.", container, blob);
            CloudBlockBlob azureBlob = blob_container.getBlockBlobReference(blob);
            azureBlob.delete();
        }
    }

    @Override
    public InputStream getInputStream(String container, String blob) throws URISyntaxException, StorageException {
        logger.trace("reading container [{}], blob [{}]", container, blob);
        return client.getContainerReference(container).getBlockBlobReference(blob).openInputStream();
    }

    @Override
    public OutputStream getOutputStream(String container, String blob) throws URISyntaxException, StorageException {
        logger.trace("writing container [{}], blob [{}]", container, blob);
        return client.getContainerReference(container).getBlockBlobReference(blob).openOutputStream();
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(String container, String keyPath, String prefix) throws URISyntaxException, StorageException {
        logger.debug("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix);
        MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();

        CloudBlobContainer blobContainer = client.getContainerReference(container);
        if (blobContainer.exists()) {
            for (ListBlobItem blobItem : blobContainer.listBlobs(keyPath + (prefix == null ? "" : prefix))) {
                URI uri = blobItem.getUri();
                logger.trace("blob url [{}]", uri);

                // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
                // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
                String blobPath = uri.getPath().substring(1 + container.length() + 1);

                CloudBlockBlob blob = blobContainer.getBlockBlobReference(blobPath);

                // fetch the blob attributes from Azure (getBlockBlobReference does not do this)
                // this is needed to retrieve the blob length (among other metadata) from Azure Storage
                blob.downloadAttributes();

                BlobProperties properties = blob.getProperties();
                String name = blobPath.substring(keyPath.length());
                logger.trace("blob url [{}], name [{}], size [{}]", uri, name, properties.getLength());
                blobsBuilder.put(name, new PlainBlobMetaData(name, properties.getLength()));
            }
        }

        return blobsBuilder.immutableMap();
    }

    @Override
    public void moveBlob(String container, String sourceBlob, String targetBlob) throws URISyntaxException, StorageException {
        logger.debug("moveBlob container [{}], sourceBlob [{}], targetBlob [{}]", container, sourceBlob, targetBlob);
        CloudBlobContainer blob_container = client.getContainerReference(container);
        CloudBlockBlob blobSource = blob_container.getBlockBlobReference(sourceBlob);
        if (blobSource.exists()) {
            CloudBlockBlob blobTarget = blob_container.getBlockBlobReference(targetBlob);
            blobTarget.startCopyFromBlob(blobSource);
            blobSource.delete();
            logger.debug("moveBlob container [{}], sourceBlob [{}], targetBlob [{}] -> done", container, sourceBlob, targetBlob);
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.debug("starting azure storage client instance");
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.debug("stopping azure storage client instance");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
