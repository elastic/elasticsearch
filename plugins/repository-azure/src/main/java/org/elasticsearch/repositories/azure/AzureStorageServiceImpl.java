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

package org.elasticsearch.repositories.azure;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.repositories.RepositoryException;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

public class AzureStorageServiceImpl extends AbstractComponent implements AzureStorageService {

    // 'package' for testing
    volatile Map<String, AzureStorageSettings> storageSettings = emptyMap();

    public AzureStorageServiceImpl(Settings settings) {
        super(settings);
        // eagerly load client settings so that secure settings are read
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        refreshAndClearCache(clientsSettings);
    }

    @Override
    public Tuple<CloudBlobClient, Supplier<OperationContext>> client(String clientName) {
        final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }
        try {
            return new Tuple<>(buildClient(azureStorageSettings), () -> buildOperationContext(azureStorageSettings));
        } catch (InvalidKeyException | URISyntaxException | IllegalArgumentException e) {
            throw new SettingsException("Invalid azure client settings with name [" + clientName + "]", e);
        }
    }

    protected CloudBlobClient buildClient(AzureStorageSettings azureStorageSettings) throws InvalidKeyException, URISyntaxException {
        final CloudBlobClient client = createClient(azureStorageSettings);
        // Set timeout option if the user sets cloud.azure.storage.timeout or
        // cloud.azure.storage.xxx.timeout (it's negative by default)
        final long timeout = azureStorageSettings.getTimeout().getMillis();
        if (timeout > 0) {
            if (timeout > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Timeout [" + azureStorageSettings.getTimeout() + "] exceeds 2,147,483,647ms.");
            }
            client.getDefaultRequestOptions().setTimeoutIntervalInMs((int) timeout);
        }
        // We define a default exponential retry policy
        client.getDefaultRequestOptions()
                .setRetryPolicyFactory(new RetryExponentialRetry(RetryPolicy.DEFAULT_CLIENT_BACKOFF, azureStorageSettings.getMaxRetries()));
        client.getDefaultRequestOptions().setLocationMode(azureStorageSettings.getLocationMode());
        return client;
    }

    protected CloudBlobClient createClient(AzureStorageSettings azureStorageSettings) throws InvalidKeyException, URISyntaxException {
        final String connectionString = azureStorageSettings.buildConnectionString();
        return CloudStorageAccount.parse(connectionString).createCloudBlobClient();
    }

    protected OperationContext buildOperationContext(AzureStorageSettings azureStorageSettings) {
        final OperationContext context = new OperationContext();
        context.setProxy(azureStorageSettings.getProxy());
        return context;
    }

    @Override
    public Map<String, AzureStorageSettings> refreshAndClearCache(Map<String, AzureStorageSettings> clientsSettings) {
        final Map<String, AzureStorageSettings> prevSettings = this.storageSettings;
        this.storageSettings = MapBuilder.newMapBuilder(clientsSettings).immutableMap();
        // clients are built lazily by {@link client(String)}
        return prevSettings;
    }

    @Override
    public boolean doesContainerExist(String account, String container) throws URISyntaxException, StorageException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client(account);
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        return SocketAccess.doPrivilegedException(() -> blobContainer.exists(null, null, client.v2().get()));
    }

    @Override
    public void removeContainer(String account, String container) throws URISyntaxException, StorageException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client(account);
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        logger.trace(() -> new ParameterizedMessage("removing container [{}]", container));
        SocketAccess.doPrivilegedException(() -> blobContainer.deleteIfExists(null, null, client.v2().get()));
    }

    @Override
    public void createContainer(String account, String container) throws URISyntaxException, StorageException {
        try {
            final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client(account);
            final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
            logger.trace(() -> new ParameterizedMessage("creating container [{}]", container));
            SocketAccess.doPrivilegedException(() -> blobContainer.createIfNotExists(null, null, client.v2().get()));
        } catch (final IllegalArgumentException e) {
            logger.trace(() -> new ParameterizedMessage("failed creating container [{}]", container), e);
            throw new RepositoryException(container, e.getMessage(), e);
        }
    }

    @Override
    public void deleteFiles(String account, String container, String path) throws URISyntaxException, StorageException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client(account);
        // container name must be lower case.
        logger.trace(() -> new ParameterizedMessage("delete files container [{}], path [{}]", container, path));
        SocketAccess.doPrivilegedVoidException(() -> {
            // list the blobs using a flat blob listing mode
            final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
            for (final ListBlobItem blobItem : blobContainer.listBlobs(path, true, EnumSet.noneOf(BlobListingDetails.class), null,
                    client.v2().get())) {
                final String blobName = blobNameFromUri(blobItem.getUri());
                logger.trace(() -> new ParameterizedMessage("removing blob [{}] full URI was [{}]", blobName, blobItem.getUri()));
                // don't call {@code #deleteBlob}, use the same client
                final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blobName);
                azureBlob.delete(DeleteSnapshotsOption.NONE, null, null, client.v2().get());
            }
        });
    }

    /**
     * Extract the blob name from a URI like https://myservice.azure.net/container/path/to/myfile
     * It should remove the container part (first part of the path) and gives path/to/myfile
     * @param uri URI to parse
     * @return The blob name relative to the container
     */
    static String blobNameFromUri(URI uri) {
        final String path = uri.getPath();
        // We remove the container name from the path
        // The 3 magic number cames from the fact if path is /container/path/to/myfile
        // First occurrence is empty "/"
        // Second occurrence is "container
        // Last part contains "path/to/myfile" which is what we want to get
        final String[] splits = path.split("/", 3);
        // We return the remaining end of the string
        return splits[2];
    }

    @Override
    public boolean blobExists(String account, String container, String blob)
            throws URISyntaxException, StorageException {
        // Container name must be lower case.
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client(account);
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        return SocketAccess.doPrivilegedException(() -> {
            final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
            return azureBlob.exists(null, null, client.v2().get());
        });
    }

    @Override
    public void deleteBlob(String account, String container, String blob) throws URISyntaxException, StorageException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client(account);
        // Container name must be lower case.
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        logger.trace(() -> new ParameterizedMessage("delete blob for container [{}], blob [{}]", container, blob));
        SocketAccess.doPrivilegedVoidException(() -> {
            final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
            logger.trace(() -> new ParameterizedMessage("container [{}]: blob [{}] found. removing.", container, blob));
            azureBlob.delete(DeleteSnapshotsOption.NONE, null, null, client.v2().get());
        });
    }

    @Override
    public InputStream getInputStream(String account, String container, String blob) throws URISyntaxException,
        StorageException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client(account);
        final CloudBlockBlob blockBlobReference = client.v1().getContainerReference(container).getBlockBlobReference(blob);
        logger.trace(() -> new ParameterizedMessage("reading container [{}], blob [{}]", container, blob));
        final BlobInputStream is = SocketAccess.doPrivilegedException(() ->
        blockBlobReference.openInputStream(null, null, client.v2().get()));
        return AzureStorageService.giveSocketPermissionsToStream(is);
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(String account, String container, String keyPath, String prefix)
        throws URISyntaxException, StorageException {
        // NOTE: this should be here: if (prefix == null) prefix = "";
        // however, this is really inefficient since deleteBlobsByPrefix enumerates everything and
        // then does a prefix match on the result; it should just call listBlobsByPrefix with the prefix!
        final MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();
        final EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client(account);
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        logger.trace(() -> new ParameterizedMessage("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix));
        SocketAccess.doPrivilegedVoidException(() -> {
            for (final ListBlobItem blobItem : blobContainer.listBlobs(keyPath + (prefix == null ? "" : prefix), false,
                    enumBlobListingDetails, null, client.v2().get())) {
                final URI uri = blobItem.getUri();
                logger.trace(() -> new ParameterizedMessage("blob url [{}]", uri));
                // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
                // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
                final String blobPath = uri.getPath().substring(1 + container.length() + 1);
                final BlobProperties properties = ((CloudBlockBlob) blobItem).getProperties();
                final String name = blobPath.substring(keyPath.length());
                logger.trace(() -> new ParameterizedMessage("blob url [{}], name [{}], size [{}]", uri, name, properties.getLength()));
                blobsBuilder.put(name, new PlainBlobMetaData(name, properties.getLength()));
            }
        });
        return blobsBuilder.immutableMap();
    }

    @Override
    public void writeBlob(String account, String container, String blobName, InputStream inputStream, long blobSize)
        throws URISyntaxException, StorageException, FileAlreadyExistsException {
        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {})", blobName, blobSize));
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client(account);
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        final CloudBlockBlob blob = blobContainer.getBlockBlobReference(blobName);
        try {
            SocketAccess.doPrivilegedVoidException(() ->
                blob.upload(inputStream, blobSize, AccessCondition.generateIfNotExistsCondition(), null, client.v2().get()));
        } catch (final StorageException se) {
            if (se.getHttpStatusCode() == HttpURLConnection.HTTP_CONFLICT &&
                StorageErrorCodeStrings.BLOB_ALREADY_EXISTS.equals(se.getErrorCode())) {
                throw new FileAlreadyExistsException(blobName, null, se.getMessage());
            }
            throw se;
        }
        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobName, blobSize));
    }

}
