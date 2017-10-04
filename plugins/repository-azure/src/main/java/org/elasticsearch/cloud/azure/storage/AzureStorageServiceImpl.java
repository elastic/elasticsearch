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
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cloud.azure.blobstore.util.SocketAccess;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryException;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class AzureStorageServiceImpl extends AbstractComponent implements AzureStorageService {

    final Map<String, AzureStorageSettings> storageSettings;
    final Map<String, AzureStorageSettings> deprecatedStorageSettings;

    final Map<String, CloudBlobClient> clients;

    public AzureStorageServiceImpl(Settings settings, Map<String, AzureStorageSettings> regularStorageSettings) {
        super(settings);

        if (regularStorageSettings.isEmpty()) {
            this.storageSettings = new HashMap<>();
            // We have deprecated settings so we need to migrate them to the new implementation
            Tuple<AzureStorageSettings, Map<String, AzureStorageSettings>> storageSettingsMapTuple = AzureStorageSettings.loadLegacy(settings);
            deprecatedStorageSettings = storageSettingsMapTuple.v2();
            if (storageSettingsMapTuple.v1() != null) {
                if (storageSettingsMapTuple.v1().getName().equals("default") == false) {
                    // We add the primary configuration to the list of all settings with its deprecated name in case someone is
                    // forcing a specific configuration name when creating the repository instance
                    deprecatedStorageSettings.put(storageSettingsMapTuple.v1().getName(), storageSettingsMapTuple.v1());
                }
                // We add the primary configuration to the list of all settings as the "default" one
                deprecatedStorageSettings.put("default", storageSettingsMapTuple.v1());
            } else {
                // If someone did not register any settings or deprecated settings, they
                // basically can't use the plugin
                throw new IllegalArgumentException("If you want to use an azure repository, you need to define a client configuration.");
            }


        } else {
            this.storageSettings = regularStorageSettings;
            this.deprecatedStorageSettings = new HashMap<>();
        }

        this.clients = new HashMap<>();

        logger.debug("starting azure storage client instance");

        // We register all regular azure clients
        for (Map.Entry<String, AzureStorageSettings> azureStorageSettingsEntry : this.storageSettings.entrySet()) {
            logger.debug("registering regular client for account [{}]", azureStorageSettingsEntry.getKey());
            createClient(azureStorageSettingsEntry.getValue());
        }

        // We register all deprecated azure clients
        for (Map.Entry<String, AzureStorageSettings> azureStorageSettingsEntry : this.deprecatedStorageSettings.entrySet()) {
            logger.debug("registering deprecated client for account [{}]", azureStorageSettingsEntry.getKey());
            createClient(azureStorageSettingsEntry.getValue());
        }
    }

    void createClient(AzureStorageSettings azureStorageSettings) {
        try {
            logger.trace("creating new Azure storage client using account [{}], key [{}], endpoint suffix [{}]",
                azureStorageSettings.getAccount(), azureStorageSettings.getKey(), azureStorageSettings.getEndpointSuffix());

            String storageConnectionString =
                "DefaultEndpointsProtocol=https;"
                    + "AccountName=" + azureStorageSettings.getAccount() + ";"
                    + "AccountKey=" + azureStorageSettings.getKey();

            String endpointSuffix = azureStorageSettings.getEndpointSuffix();
            if (endpointSuffix != null && !endpointSuffix.isEmpty()) {
                storageConnectionString += ";EndpointSuffix=" + endpointSuffix;
            }
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
        AzureStorageSettings azureStorageSettings = this.storageSettings.get(account);
        if (azureStorageSettings == null) {
            // We can't find a client that has been registered using regular settings so we try deprecated client
            azureStorageSettings = this.deprecatedStorageSettings.get(account);
            if (azureStorageSettings == null) {
                // We did not get an account. That's bad.
                if (Strings.hasLength(account)) {
                    throw new IllegalArgumentException("Can not find named azure client [" + account +
                        "]. Check your elasticsearch.yml.");
                }
                throw new IllegalArgumentException("Can not find primary/secondary client using deprecated settings. " +
                    "Check your elasticsearch.yml.");
            }
        }

        CloudBlobClient client = this.clients.get(azureStorageSettings.getAccount());

        if (client == null) {
            throw new IllegalArgumentException("Can not find an azure client for account [" + azureStorageSettings.getAccount() + "]");
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

        // We define a default exponential retry policy
        client.getDefaultRequestOptions().setRetryPolicyFactory(
            new RetryExponentialRetry(RetryPolicy.DEFAULT_CLIENT_BACKOFF, azureStorageSettings.getMaxRetries()));

        return client;
    }

    private OperationContext generateOperationContext(String clientName) {
        OperationContext context = new OperationContext();
        AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);

        if (azureStorageSettings.getProxy() != null) {
            context.setProxy(azureStorageSettings.getProxy());
        }

        return context;
    }

    @Override
    public boolean doesContainerExist(String account, LocationMode mode, String container) {
        try {
            CloudBlobClient client = this.getSelectedClient(account, mode);
            CloudBlobContainer blobContainer = client.getContainerReference(container);
            return SocketAccess.doPrivilegedException(() -> blobContainer.exists(null, null, generateOperationContext(account)));
        } catch (Exception e) {
            logger.error("can not access container [{}]", container);
        }
        return false;
    }

    @Override
    public void removeContainer(String account, LocationMode mode, String container) throws URISyntaxException, StorageException {
        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        logger.trace("removing container [{}]", container);
        SocketAccess.doPrivilegedException(() -> blobContainer.deleteIfExists(null, null, generateOperationContext(account)));
    }

    @Override
    public void createContainer(String account, LocationMode mode, String container) throws URISyntaxException, StorageException {
        try {
            CloudBlobClient client = this.getSelectedClient(account, mode);
            CloudBlobContainer blobContainer = client.getContainerReference(container);
            logger.trace("creating container [{}]", container);
            SocketAccess.doPrivilegedException(() -> blobContainer.createIfNotExists(null, null, generateOperationContext(account)));
        } catch (IllegalArgumentException e) {
            logger.trace((Supplier<?>) () -> new ParameterizedMessage("fails creating container [{}]", container), e);
            throw new RepositoryException(container, e.getMessage(), e);
        }
    }

    @Override
    public void deleteFiles(String account, LocationMode mode, String container, String path) throws URISyntaxException, StorageException {
        logger.trace("delete files container [{}], path [{}]", container, path);

        // Container name must be lower case.
        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        SocketAccess.doPrivilegedVoidException(() -> {
            if (blobContainer.exists()) {
                // We list the blobs using a flat blob listing mode
                for (ListBlobItem blobItem : blobContainer.listBlobs(path, true, EnumSet.noneOf(BlobListingDetails.class), null,
                    generateOperationContext(account))) {
                    String blobName = blobNameFromUri(blobItem.getUri());
                    logger.trace("removing blob [{}] full URI was [{}]", blobName, blobItem.getUri());
                    deleteBlob(account, mode, container, blobName);
                }
            }
        });
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
        if (SocketAccess.doPrivilegedException(() -> blobContainer.exists(null, null, generateOperationContext(account)))) {
            CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
            return SocketAccess.doPrivilegedException(() -> azureBlob.exists(null, null, generateOperationContext(account)));
        }

        return false;
    }

    @Override
    public void deleteBlob(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException {
        logger.trace("delete blob for container [{}], blob [{}]", container, blob);

        // Container name must be lower case.
        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        if (SocketAccess.doPrivilegedException(() -> blobContainer.exists(null, null, generateOperationContext(account)))) {
            logger.trace("container [{}]: blob [{}] found. removing.", container, blob);
            CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
            SocketAccess.doPrivilegedVoidException(() -> azureBlob.delete(DeleteSnapshotsOption.NONE, null, null,
                generateOperationContext(account)));
        }
    }

    @Override
    public InputStream getInputStream(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException {
        logger.trace("reading container [{}], blob [{}]", container, blob);
        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlockBlob blockBlobReference = client.getContainerReference(container).getBlockBlobReference(blob);
        return SocketAccess.doPrivilegedException(() -> blockBlobReference.openInputStream(null, null, generateOperationContext(account)));
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(String account, LocationMode mode, String container, String keyPath, String prefix) throws URISyntaxException, StorageException {
        // NOTE: this should be here: if (prefix == null) prefix = "";
        // however, this is really inefficient since deleteBlobsByPrefix enumerates everything and
        // then does a prefix match on the result; it should just call listBlobsByPrefix with the prefix!

        logger.debug("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix);
        MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();
        EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);
        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        SocketAccess.doPrivilegedVoidException(() -> {
            if (blobContainer.exists()) {
                for (ListBlobItem blobItem : blobContainer.listBlobs(keyPath + (prefix == null ? "" : prefix), false,
                    enumBlobListingDetails, null, generateOperationContext(account))) {
                    URI uri = blobItem.getUri();
                    logger.trace("blob url [{}]", uri);

                    // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
                    // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
                    String blobPath = uri.getPath().substring(1 + container.length() + 1);
                    BlobProperties properties = ((CloudBlockBlob) blobItem).getProperties();
                    String name = blobPath.substring(keyPath.length());
                    logger.trace("blob url [{}], name [{}], size [{}]", uri, name, properties.getLength());
                    blobsBuilder.put(name, new PlainBlobMetaData(name, properties.getLength()));
                }
            }
        });
        return blobsBuilder.immutableMap();
    }

    @Override
    public void moveBlob(String account, LocationMode mode, String container, String sourceBlob, String targetBlob) throws URISyntaxException, StorageException {
        logger.debug("moveBlob container [{}], sourceBlob [{}], targetBlob [{}]", container, sourceBlob, targetBlob);

        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        CloudBlockBlob blobSource = blobContainer.getBlockBlobReference(sourceBlob);
        if (SocketAccess.doPrivilegedException(() -> blobSource.exists(null, null, generateOperationContext(account)))) {
            CloudBlockBlob blobTarget = blobContainer.getBlockBlobReference(targetBlob);
            SocketAccess.doPrivilegedVoidException(() -> {
                blobTarget.startCopy(blobSource, null, null, null, generateOperationContext(account));
                blobSource.delete(DeleteSnapshotsOption.NONE, null, null, generateOperationContext(account));
            });
            logger.debug("moveBlob container [{}], sourceBlob [{}], targetBlob [{}] -> done", container, sourceBlob, targetBlob);
        }
    }

    @Override
    public void writeBlob(String account, LocationMode mode, String container, String blobName, InputStream inputStream, long blobSize)
        throws URISyntaxException, StorageException {
        logger.trace("writeBlob({}, stream, {})", blobName, blobSize);
        CloudBlobClient client = this.getSelectedClient(account, mode);
        CloudBlobContainer blobContainer = client.getContainerReference(container);
        CloudBlockBlob blob = blobContainer.getBlockBlobReference(blobName);
        SocketAccess.doPrivilegedVoidException(() -> blob.upload(inputStream, blobSize, null, null, generateOperationContext(account)));
        logger.trace("writeBlob({}, stream, {}) - done", blobName, blobSize);
    }
}
