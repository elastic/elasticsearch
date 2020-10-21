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
import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RequestCompletedEvent;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageEvent;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

public class AzureBlobStore implements BlobStore {

    private static final Logger logger = LogManager.getLogger(AzureBlobStore.class);

    private final AzureStorageService service;
    private final ThreadPool threadPool;

    private final String clientName;
    private final String container;
    private final LocationMode locationMode;

    private final Stats stats = new Stats();

    private final Consumer<HttpURLConnection> getMetricsCollector;
    private final Consumer<HttpURLConnection> listMetricsCollector;
    private final Consumer<HttpURLConnection> uploadMetricsCollector;

    public AzureBlobStore(RepositoryMetadata metadata, AzureStorageService service, ThreadPool threadPool) {
        this.container = Repository.CONTAINER_SETTING.get(metadata.settings());
        this.clientName = Repository.CLIENT_NAME.get(metadata.settings());
        this.service = service;
        this.threadPool = threadPool;
        // locationMode is set per repository, not per client
        this.locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        final Map<String, AzureStorageSettings> prevSettings = this.service.refreshAndClearCache(emptyMap());
        final Map<String, AzureStorageSettings> newSettings = AzureStorageSettings.overrideLocationMode(prevSettings, this.locationMode);
        this.service.refreshAndClearCache(newSettings);
        this.getMetricsCollector = (httpURLConnection) -> {
            if (httpURLConnection.getRequestMethod().equals("HEAD")) {
                stats.headOperations.incrementAndGet();
                return;
            }
            assert httpURLConnection.getRequestMethod().equals("GET");

            stats.getOperations.incrementAndGet();
        };
        this.listMetricsCollector = (httpURLConnection) -> {
            assert httpURLConnection.getRequestMethod().equals("GET");
            stats.listOperations.incrementAndGet();
        };
        this.uploadMetricsCollector = (httpURLConnection -> {
           assert httpURLConnection.getRequestMethod().equals("PUT");
            String queryParams = httpURLConnection.getURL().getQuery() == null ? "" : httpURLConnection.getURL().getQuery();

            // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block
            // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
            if (queryParams.contains("comp=block") && queryParams.contains("blockid=")) {
                stats.putBlockOperations.incrementAndGet();
            } else if (queryParams.contains("comp=blocklist")) {
                stats.putBlockListOperations.incrementAndGet();
            } else {
                // https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#uri-parameters
                // The only URI parameter allowed for put-blob operation is "timeout", but if a sas token is used,
                // it's possible that the URI parameters contain additional parameters unrelated to the upload type.
                stats.putOperations.incrementAndGet();
            }
        });
    }

    @Override
    public String toString() {
        return container;
    }

    public AzureStorageService getService() {
        return service;
    }

    /**
     * Gets the configured {@link LocationMode} for the Azure storage requests.
     */
    public LocationMode getLocationMode() {
        return locationMode;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new AzureBlobContainer(path, this, threadPool);
    }

    @Override
    public void close() {
    }

    public boolean blobExists(String blob) throws URISyntaxException, StorageException {
        // Container name must be lower case.
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final OperationContext context = hookMetricCollector(client.v2().get(), getMetricsCollector);
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        return SocketAccess.doPrivilegedException(() -> {
            final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
            return azureBlob.exists(null, null, context);
        });
    }

    public void deleteBlob(String blob) throws URISyntaxException, StorageException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        // Container name must be lower case.
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        logger.trace(() -> new ParameterizedMessage("delete blob for container [{}], blob [{}]", container, blob));
        SocketAccess.doPrivilegedVoidException(() -> {
            final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
            logger.trace(() -> new ParameterizedMessage("container [{}]: blob [{}] found. removing.", container, blob));
            azureBlob.delete(DeleteSnapshotsOption.NONE, null, null, client.v2().get());
        });
    }

    public DeleteResult deleteBlobDirectory(String path, Executor executor)
            throws URISyntaxException, StorageException, IOException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final OperationContext context = hookMetricCollector(client.v2().get(), listMetricsCollector);
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        final Collection<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        final AtomicLong outstanding = new AtomicLong(1L);
        final PlainActionFuture<Void> result = PlainActionFuture.newFuture();
        final AtomicLong blobsDeleted = new AtomicLong();
        final AtomicLong bytesDeleted = new AtomicLong();
        SocketAccess.doPrivilegedVoidException(() -> {
            for (final ListBlobItem blobItem : blobContainer.listBlobs(path, true,
                EnumSet.noneOf(BlobListingDetails.class), null, context)) {
                // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
                // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
                final String blobPath = blobItem.getUri().getPath().substring(1 + container.length() + 1);
                outstanding.incrementAndGet();
                executor.execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        final long len;
                        if (blobItem instanceof CloudBlob) {
                            len = ((CloudBlob) blobItem).getProperties().getLength();
                        } else {
                            len = -1L;
                        }
                        deleteBlob(blobPath);
                        blobsDeleted.incrementAndGet();
                        if (len >= 0) {
                            bytesDeleted.addAndGet(len);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        exceptions.add(e);
                    }

                    @Override
                    public void onAfter() {
                        if (outstanding.decrementAndGet() == 0) {
                            result.onResponse(null);
                        }
                    }
                });
            }
        });
        if (outstanding.decrementAndGet() == 0) {
            result.onResponse(null);
        }
        result.actionGet();
        if (exceptions.isEmpty() == false) {
            final IOException ex = new IOException("Deleting directory [" + path + "] failed");
            exceptions.forEach(ex::addSuppressed);
            throw ex;
        }
        return new DeleteResult(blobsDeleted.get(), bytesDeleted.get());
    }

    public InputStream getInputStream(String blob, long position, @Nullable Long length) throws URISyntaxException, StorageException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final OperationContext context = hookMetricCollector(client.v2().get(), getMetricsCollector);
        final CloudBlockBlob blockBlobReference = client.v1().getContainerReference(container).getBlockBlobReference(blob);
        logger.trace(() -> new ParameterizedMessage("reading container [{}], blob [{}]", container, blob));
        final long limit;
        if (length == null) {
            // Loading the blob attributes so we can get its length
            SocketAccess.doPrivilegedVoidException(() -> blockBlobReference.downloadAttributes(null, null, context));
            limit = blockBlobReference.getProperties().getLength() - position;
        }
        else {
            limit = length;
        }
        final BlobInputStream blobInputStream = new BlobInputStream(limit, blockBlobReference, position, context);
        if (length != null) {
            // pre-filling the buffer in case of ranged reads so this method throws a 404 storage exception right away in case the blob
            // does not exist
            blobInputStream.fill();
        }
        return blobInputStream;
    }

    public Map<String, BlobMetadata> listBlobsByPrefix(String keyPath, String prefix)
        throws URISyntaxException, StorageException {
        // NOTE: this should be here: if (prefix == null) prefix = "";
        // however, this is really inefficient since deleteBlobsByPrefix enumerates everything and
        // then does a prefix match on the result; it should just call listBlobsByPrefix with the prefix!
        final var blobsBuilder = new HashMap<String, BlobMetadata>();
        final EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final OperationContext context = hookMetricCollector(client.v2().get(), listMetricsCollector);
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        logger.trace(() -> new ParameterizedMessage("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix));
        SocketAccess.doPrivilegedVoidException(() -> {
            for (final ListBlobItem blobItem : blobContainer.listBlobs(keyPath + (prefix == null ? "" : prefix), false,
                enumBlobListingDetails, null, context)) {
                final URI uri = blobItem.getUri();
                logger.trace(() -> new ParameterizedMessage("blob url [{}]", uri));
                // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
                // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
                final String blobPath = uri.getPath().substring(1 + container.length() + 1);
                if (blobItem instanceof CloudBlob) {
                    final BlobProperties properties = ((CloudBlob) blobItem).getProperties();
                    final String name = blobPath.substring(keyPath.length());
                    logger.trace(() -> new ParameterizedMessage("blob url [{}], name [{}], size [{}]", uri, name, properties.getLength()));
                    blobsBuilder.put(name, new PlainBlobMetadata(name, properties.getLength()));
                }
            }
        });
        return Map.copyOf(blobsBuilder);
    }

    public Map<String, BlobContainer> children(BlobPath path) throws URISyntaxException, StorageException {
        final var blobsBuilder = new HashSet<String>();
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final OperationContext context = hookMetricCollector(client.v2().get(), listMetricsCollector);
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        final String keyPath = path.buildAsString();
        final EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);

        SocketAccess.doPrivilegedVoidException(() -> {
            for (ListBlobItem blobItem : blobContainer.listBlobs(keyPath, false, enumBlobListingDetails, null, context)) {
                if (blobItem instanceof CloudBlobDirectory) {
                    final URI uri = blobItem.getUri();
                    logger.trace(() -> new ParameterizedMessage("blob url [{}]", uri));
                    // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
                    // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /.
                    // Lastly, we add the length of keyPath to the offset to strip this container's path.
                    final String uriPath = uri.getPath();
                    blobsBuilder.add(uriPath.substring(1 + container.length() + 1 + keyPath.length(), uriPath.length() - 1));
                }
            }
        });

        return Collections.unmodifiableMap(blobsBuilder.stream().collect(
            Collectors.toMap(Function.identity(), name -> new AzureBlobContainer(path.add(name), this, threadPool))));
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws URISyntaxException, StorageException, IOException {
        assert inputStream.markSupported()
            : "Should not be used with non-mark supporting streams as their retry handling in the SDK is broken";
        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {})", blobName, blobSize));
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final OperationContext operationContext = hookMetricCollector(client().v2().get(), uploadMetricsCollector);
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        final CloudBlockBlob blob = blobContainer.getBlockBlobReference(blobName);
        try {
            final AccessCondition accessCondition =
                failIfAlreadyExists ? AccessCondition.generateIfNotExistsCondition() : AccessCondition.generateEmptyCondition();
            SocketAccess.doPrivilegedVoidException(() ->
                blob.upload(inputStream, blobSize, accessCondition, service.getBlobRequestOptionsForWriteBlob(), operationContext));
        } catch (final StorageException se) {
            if (failIfAlreadyExists && se.getHttpStatusCode() == HttpURLConnection.HTTP_CONFLICT &&
                StorageErrorCodeStrings.BLOB_ALREADY_EXISTS.equals(se.getErrorCode())) {
                throw new FileAlreadyExistsException(blobName, null, se.getMessage());
            }
            throw se;
        }
        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobName, blobSize));
    }

    private Tuple<CloudBlobClient, Supplier<OperationContext>> client() {
        return service.client(clientName);
    }

    private OperationContext hookMetricCollector(OperationContext context, Consumer<HttpURLConnection> metricCollector) {
        context.getRequestCompletedEventHandler().addListener(new StorageEvent<>() {
            @Override
            public void eventOccurred(RequestCompletedEvent eventArg) {
                int statusCode = eventArg.getRequestResult().getStatusCode();
                if (statusCode < 300) {
                    metricCollector.accept((HttpURLConnection) eventArg.getConnectionObject());
                }
            }
        });
        return context;
    }

    @Override
    public Map<String, Long> stats() {
        return stats.toMap();
    }

    private static class Stats {

        private final AtomicLong getOperations = new AtomicLong();

        private final AtomicLong listOperations = new AtomicLong();

        private final AtomicLong headOperations = new AtomicLong();

        private final AtomicLong putOperations = new AtomicLong();

        private final AtomicLong putBlockOperations = new AtomicLong();

        private final AtomicLong putBlockListOperations = new AtomicLong();

        private Map<String, Long> toMap() {
            return Map.of("GetBlob", getOperations.get(),
                "ListBlobs", listOperations.get(),
                "GetBlobProperties", headOperations.get(),
                "PutBlob", putOperations.get(),
                "PutBlock", putBlockOperations.get(),
                "PutBlockList", putBlockListOperations.get());
        }
    }

    /**
     * Building our own input stream instead of using the SDK's {@link com.microsoft.azure.storage.blob.BlobInputStream}
     * because that stream is highly inefficient in both memory and CPU use.
     */
    private static class BlobInputStream extends InputStream {

        /**
         * Maximum number of bytes to fetch per read request and thus to buffer on heap at a time.
         * Set to 4M because that's what {@link com.microsoft.azure.storage.blob.BlobInputStream} uses.
         */
        private static final int MAX_READ_CHUNK_SIZE = ByteSizeUnit.MB.toIntBytes(4);

        /**
         * Using a {@link ByteArrayOutputStream} as a buffer instead of a byte array since the byte array APIs on the SDK are less
         * efficient.
         */
        private final ByteArrayOutputStream buffer;

        private final long limit;

        private final CloudBlockBlob blockBlobReference;

        private final long start;

        private final OperationContext context;

        // current read position on the byte array backing #buffer
        private int pos;

        // current position up to which the contents of the blob where buffered
        private long offset;

        BlobInputStream(long limit, CloudBlockBlob blockBlobReference, long start, OperationContext context) {
            this.limit = limit;
            this.blockBlobReference = blockBlobReference;
            this.start = start;
            this.context = context;
            buffer = new ByteArrayOutputStream(Math.min(MAX_READ_CHUNK_SIZE, Math.toIntExact(Math.min(limit, Integer.MAX_VALUE)))) {
                @Override
                public byte[] toByteArray() {
                    return buf;
                }
            };
            pos = 0;
            offset = 0;
        }

        @Override
        public int read() throws IOException {
            try {
                fill();
            } catch (StorageException | URISyntaxException ex) {
                throw new IOException(ex);
            }
            if (pos == buffer.size()) {
                return -1;
            }
            return buffer.toByteArray()[pos++];
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                fill();
            } catch (StorageException | URISyntaxException ex) {
                throw new IOException(ex);
            }
            final int buffered = buffer.size();
            int remaining = buffered - pos;
            if (len > 0 && remaining == 0) {
                return -1;
            }
            final int toRead = Math.min(remaining, len);
            System.arraycopy(buffer.toByteArray(), pos, b, off, toRead);
            pos += toRead;
            return toRead;
        }

        void fill() throws StorageException, URISyntaxException {
            if (pos == buffer.size()) {
                final long toFill = Math.min(limit - this.offset, MAX_READ_CHUNK_SIZE);
                if (toFill <= 0L) {
                    return;
                }
                buffer.reset();
                SocketAccess.doPrivilegedVoidException(() -> blockBlobReference.downloadRange(
                        start + this.offset, toFill, buffer, null, null, context));
                this.offset += buffer.size();
                pos = 0;
            }
        }
    }
}
