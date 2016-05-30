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
import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryException;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class AzureStorageServiceImpl extends AbstractLifecycleComponent<AzureStorageServiceImpl>
    implements AzureStorageService {

    final AzureStorageSettings primaryStorageSettings;
    final Map<String, AzureStorageSettings> secondariesStorageSettings;

    final Map<String, CloudBlobClient> clients;

    @Inject
    public AzureStorageServiceImpl(Settings settings) {
        super(settings);

        Tuple<AzureStorageSettings, Map<String, AzureStorageSettings>> storageSettings = AzureStorageSettings.parse(settings);
        this.primaryStorageSettings = storageSettings.v1();
        this.secondariesStorageSettings = storageSettings.v2();

        this.clients = new HashMap<>();
    }

    void createClient(AzureStorageSettings azureStorageSettings) {
        try {
            logger.trace("creating new Azure storage client using account [{}], key [{}]",
                    azureStorageSettings.getAccount(), azureStorageSettings.getKey());

            String storageConnectionString =
                    "DefaultEndpointsProtocol=https;"
                            + "AccountName="+ azureStorageSettings.getAccount() +";"
                            + "AccountKey=" + azureStorageSettings.getKey();

            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient client = storageAccount.createCloudBlobClient();

            // Register the client
            this.clients.put(azureStorageSettings.getAccount(), client);
        } catch (Exception e) {
            logger.error("can not create azure storage client: {}", e.getMessage());
        }
    }

    CloudBlobClient getSelectedClient(String account, LocationMode mode) {
        logger.trace("selecting a client for account [{}], mode [{}]", account, mode.name());
        AzureStorageSettings azureStorageSettings = null;

        if (this.primaryStorageSettings == null) {
            throw new IllegalArgumentException("No primary azure storage can be found. Check your elasticsearch.yml.");
        }

        if (Strings.hasLength(account)) {
            azureStorageSettings = this.secondariesStorageSettings.get(account);
        }

        // if account is not secondary, it's the primary
        if (azureStorageSettings == null) {
            if (Strings.hasLength(account) == false || primaryStorageSettings.getName() == null || account.equals(primaryStorageSettings.getName())) {
                azureStorageSettings = primaryStorageSettings;
            }
        }

        if (azureStorageSettings == null) {
            // We did not get an account. That's bad.
            throw new IllegalArgumentException("Can not find azure account [" + account + "]. Check your elasticsearch.yml.");
        }

        CloudBlobClient client = this.clients.get(azureStorageSettings.getAccount());

        if (client == null) {
            throw new IllegalArgumentException("Can not find an azure client for account [" + account + "]");
        }

        // NOTE: for now, just set the location mode in case it is different;
        // only one mode per storage account can be active at a time
        client.getDefaultRequestOptions().setLocationMode(mode);

        // Set timeout option if the user sets cloud.azure.storage.timeout or cloud.azure.storage.xxx.timeout (it's negative by default)
        if (azureStorageSettings.getTimeout().getSeconds() > 0) {
            try {
                int timeout = (int) azureStorageSettings.getTimeout().getMillis();
                client.getDefaultRequestOptions().setTimeoutIntervalInMs(timeout);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Can not convert [" + azureStorageSettings.getTimeout() +
                    "]. It can not be longer than 2,147,483,647ms.");
            }
        }
        return client;
    }

    @Override
    public boolean doesContainerExist(String account, LocationMode mode, String container) {
        try {
            CloudBlobClient client = this.getSelectedClient(account, mode);
            CloudBlobContainer blobContainer = client.getContainerReference(container);
            return blobContainer.exists();
        } catch (Exception e) {
            logger.error("can not access container [{}]", container);
        }
        return false;
    }

    @Override
    public void removeContainer(String account, LocationMode mode, String container) throws URISyntaxException, StorageException {
        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        // TODO Should we set some timeout and retry options?
        /*
        BlobRequestOptions options = new BlobRequestOptions();
        options.setTimeoutIntervalInMs(1000);
        options.setRetryPolicyFactory(new RetryNoRetry());
        blobContainer.deleteIfExists(options, null);
        */
        logger.trace("removing container [{}]", container);
        blobContainer.deleteIfExists();
    }

    @Override
    public void createContainer(String account, LocationMode mode, String container) throws URISyntaxException, StorageException {
        try {
            CloudBlobClient client = this.getSelectedClient(account, mode);
            CloudBlobContainer blobContainer = client.getContainerReference(container);
            logger.trace("creating container [{}]", container);
            blobContainer.createIfNotExists();
        } catch (IllegalArgumentException e) {
            logger.trace("fails creating container [{}]", e, container);
            throw new RepositoryException(container, e.getMessage());
        }
    }

    @Override
    public void deleteFiles(String account, LocationMode mode, String container, String path) throws URISyntaxException, StorageException {
        logger.trace("delete files container [{}], path [{}]", container, path);

        // Container name must be lower case.
        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        if (blobContainer.exists()) {
            // We list the blobs using a flat blob listing mode
            for (ListBlobItem blobItem : blobContainer.listBlobs(path, true)) {
                String blobName = blobNameFromUri(blobItem.getUri());
                logger.trace("removing blob [{}] full URI was [{}]", blobName, blobItem.getUri());
                deleteBlob(account, mode, container, blobName);
            }
        }
    }

    /**
     * Extract the blob name from a URI like https://myservice.azure.net/container/path/to/myfile
     * It should remove the container part (first part of the path) and gives path/to/myfile
     * @param uri URI to parse
     * @return The blob name relative to the container
     */
    public static String blobNameFromUri(URI uri) {
        String path = uri.getPath();

        // We remove the container name from the path
        // The 3 magic number cames from the fact if path is /container/path/to/myfile
        // First occurrence is empty "/"
        // Second occurrence is "container
        // Last part contains "path/to/myfile" which is what we want to get
        String[] splits = path.split("/", 3);

        // We return the remaining end of the string
        return splits[2];
    }

    @Override
    public boolean blobExists(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException {
        // Container name must be lower case.
        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        if (blobContainer.exists()) {
            CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
            return azureBlob.exists();
        }

        return false;
    }

    @Override
    public void deleteBlob(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException {
        logger.trace("delete blob for container [{}], blob [{}]", container, blob);

        // Container name must be lower case.
        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        if (blobContainer.exists()) {
            logger.trace("container [{}]: blob [{}] found. removing.", container, blob);
            CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
            azureBlob.delete();
        }
    }

    @Override
    public InputStream getInputStream(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException {
        logger.trace("reading container [{}], blob [{}]", container, blob);
        CloudBlobClient client = this.getSelectedClient(account, mode);
        return client.getContainerReference(container).getBlockBlobReference(blob).openInputStream();
    }

    @Override
    public OutputStream getOutputStream(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException {
        logger.trace("writing container [{}], blob [{}]", container, blob);
        CloudBlobClient client = this.getSelectedClient(account, mode);
        return client.getContainerReference(container).getBlockBlobReference(blob).openOutputStream();
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(String account, LocationMode mode, String container, String keyPath, String prefix) throws URISyntaxException, StorageException {
        // NOTE: this should be here: if (prefix == null) prefix = "";
        // however, this is really inefficient since deleteBlobsByPrefix enumerates everything and
        // then does a prefix match on the result; it should just call listBlobsByPrefix with the prefix!

        logger.debug("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix);
        MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();

        CloudBlobClient client = this.getSelectedClient(account, mode);
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
    public void moveBlob(String account, LocationMode mode, String container, String sourceBlob, String targetBlob) throws URISyntaxException, StorageException {
        logger.debug("moveBlob container [{}], sourceBlob [{}], targetBlob [{}]", container, sourceBlob, targetBlob);

        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        CloudBlockBlob blobSource = blobContainer.getBlockBlobReference(sourceBlob);
        if (blobSource.exists()) {
            CloudBlockBlob blobTarget = blobContainer.getBlockBlobReference(targetBlob);
            blobTarget.startCopy(blobSource);
            blobSource.delete();
            logger.debug("moveBlob container [{}], sourceBlob [{}], targetBlob [{}] -> done", container, sourceBlob, targetBlob);
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.debug("starting azure storage client instance");

        // We register the primary client if any
        if (primaryStorageSettings != null) {
            logger.debug("registering primary client for account [{}]", primaryStorageSettings.getAccount());
            createClient(primaryStorageSettings);
        }

        // We register all secondary clients
        for (Map.Entry<String, AzureStorageSettings> azureStorageSettingsEntry : secondariesStorageSettings.entrySet()) {
            logger.debug("registering secondary client for account [{}]", azureStorageSettingsEntry.getKey());
            createClient(azureStorageSettingsEntry.getValue());
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.debug("stopping azure storage client instance");
        // We should stop all clients but it does sound like CloudBlobClient has
        // any shutdown method...
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
