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

import com.azure.core.http.rest.ResponseBase;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.common.StorageInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

public class AzureBlobStore implements BlobStore {
    private static final Logger logger = LogManager.getLogger(AzureBlobStore.class);
    // See com.azure.storage.blob.specialized.BlobClientBase.openInputStream(com.azure.storage.blob.options.BlobInputStreamOptions)
    private static final long DEFAULT_READ_CHUNK_SIZE = new ByteSizeValue(4, ByteSizeUnit.MB).getBytes();

    private final AzureStorageService service;

    private final String clientName;
    private final String container;
    private final LocationMode locationMode;

    private final Stats stats = new Stats();
    private final BiConsumer<String, URL> statsConsumer;

    public AzureBlobStore(RepositoryMetadata metadata, AzureStorageService service) {
        this.container = Repository.CONTAINER_SETTING.get(metadata.settings());
        this.clientName = Repository.CLIENT_NAME.get(metadata.settings());
        this.service = service;
        // locationMode is set per repository, not per client
        this.locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());

        List<RequestStatsCollector> requestStatsCollectors = List.of(
            RequestStatsCollector.create(
                (httpMethod, url) -> httpMethod.equals("HEAD"),
                stats.headOperations::incrementAndGet
            ),
            RequestStatsCollector.create(
                (httpMethod, url) -> httpMethod.equals("GET") && isListRequest(httpMethod, url) == false,
                stats.getOperations::incrementAndGet
            ),
            RequestStatsCollector.create(
                this::isListRequest,
                stats.listOperations::incrementAndGet
            ),
            RequestStatsCollector.create(
                this::isPutBlockRequest,
                stats.putBlockOperations::incrementAndGet
            ),
            RequestStatsCollector.create(
                this::isPutBlockListRequest,
                stats.putBlockListOperations::incrementAndGet
            ),
            RequestStatsCollector.create(
                // https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#uri-parameters
                // The only URI parameter allowed for put-blob operation is "timeout", but if a sas token is used,
                // it's possible that the URI parameters contain additional parameters unrelated to the upload type.
                (httpMethod, url) -> httpMethod.equals("PUT") &&
                    isPutBlockRequest(httpMethod, url) == false &&
                    isPutBlockListRequest(httpMethod, url) == false,
                stats.putOperations::incrementAndGet
            )
        );

        this.statsConsumer = (httpMethod, url) -> {
            try {
                URI uri = url.toURI();
                String path = uri.getPath() == null ? "" : uri.getPath();
                // Batch delete requests
                if (path.contains(container) == false) {
                    return;
                }
                assert path.contains(container) : uri.toString();
            } catch (URISyntaxException ignored) {
                return;
            }

            for (RequestStatsCollector requestStatsCollector : requestStatsCollectors) {
                if (requestStatsCollector.shouldConsumeRequestInfo(httpMethod, url)) {
                    requestStatsCollector.consumeHttpRequestInfo();
                    return;
                }
            }
        };
    }

    private boolean isListRequest(String httpMethod, URL url) {
        return httpMethod.equals("GET") &&
            url.getQuery() != null &&
            url.getQuery().contains("comp=list") &&
            url.getQuery().contains("delimiter=");
    }

    // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block
    private boolean isPutBlockRequest(String httpMethod, URL url) {
        String queryParams = url.getQuery() == null ? "" : url.getQuery();
        return httpMethod.equals("PUT") &&
            queryParams.contains("comp=block") &&
            queryParams.contains("blockid=");
    }

    // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
    private boolean isPutBlockListRequest(String httpMethod, URL url) {
        String queryParams = url.getQuery() == null ? "" : url.getQuery();
        return httpMethod.equals("PUT") &&
            queryParams.contains("comp=blocklist");
    }

    public long getReadChunkSize() {
        return DEFAULT_READ_CHUNK_SIZE;
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
        return new AzureBlobContainer(path, this);
    }

    @Override
    public void close() {
    }

    public boolean blobExists(String blob) {
        final BlobServiceClient client = client();

        try {
            Boolean blobExists = SocketAccess.doPrivilegedException(() -> {
                final BlobClient azureBlob = client.getBlobContainerClient(container).getBlobClient(blob);
                return azureBlob.exists();
            });
            return blobExists != null ? blobExists : false;
        } catch (Exception e) {
            logger.warn("can not access [{}] in container {{}}: {}", blob, container, e.getMessage());
        }
        return false;
    }

    public DeleteResult deleteBlobDirectory(String path) throws IOException {
        final AtomicInteger blobsDeleted = new AtomicInteger(0);
        final AtomicLong bytesDeleted = new AtomicLong(0);

        try {
            final BlobServiceClient client = client();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobContainerClient blobContainerClient = client.getBlobContainerClient(container);
                final BlobContainerAsyncClient blobContainerAsyncClient = asyncClient().getBlobContainerAsyncClient(container);
                final Queue<String> directories = new ArrayDeque<>();
                directories.offer(path);
                String directoryName;
                List<Mono<Void>> deleteTasks = new ArrayList<>();
                while ((directoryName = directories.poll()) != null) {
                    final BlobListDetails blobListDetails = new BlobListDetails()
                        .setRetrieveMetadata(true);

                    final ListBlobsOptions options = new ListBlobsOptions()
                        .setPrefix(directoryName)
                        .setDetails(blobListDetails);

                    for (BlobItem blobItem : blobContainerClient.listBlobsByHierarchy("/", options, null)) {
                        if (blobItem.isPrefix() != null && blobItem.isPrefix()) {
                            directories.offer(blobItem.getName());
                        } else {
                            BlobAsyncClient blobAsyncClient = blobContainerAsyncClient.getBlobAsyncClient(blobItem.getName());
                            deleteTasks.add(blobAsyncClient.delete());
                            bytesDeleted.addAndGet(blobItem.getProperties().getContentLength());
                            blobsDeleted.incrementAndGet();
                        }
                    }
                }

                executeDeleteTasks(deleteTasks);
            });
        } catch (Exception e) {
            throw new IOException("Deleting directory [" + path + "] failed", e);
        }

        return new DeleteResult(blobsDeleted.get(), bytesDeleted.get());
    }

    void deleteBlobList(List<String> blobs) throws IOException {
        if (blobs.isEmpty()) {
            return;
        }

        try {
            BlobServiceAsyncClient asyncClient = asyncClient();
            SocketAccess.doPrivilegedVoidException(() -> {
                List<Mono<Void>> deleteTasks = new ArrayList<>(blobs.size());
                final BlobContainerAsyncClient blobContainerClient = asyncClient.getBlobContainerAsyncClient(container);
                for (String blob : blobs) {
                    deleteTasks.add(blobContainerClient.getBlobAsyncClient(blob).delete());
                }

                executeDeleteTasks(deleteTasks);
            });
        } catch (BlobStorageException e) {
            if (e.getStatusCode() != 404) {
                throw new IOException("Unable to delete blobs " + blobs, e);
            }
        } catch (Exception e) {
            throw new IOException("Unable to delete blobs " + blobs, e);
        }
    }

    private void executeDeleteTasks(List<Mono<Void>> deleteTasks) {
        Flux.merge(deleteTasks)
            .collectList()
            .block();
    }

    public InputStream getInputStream(String blob, long position, final @Nullable Long length) throws IOException {
        logger.trace(() -> new ParameterizedMessage("reading container [{}], blob [{}]", container, blob));
        final BlobServiceClient client = client();

        return SocketAccess.doPrivilegedException(() ->{
            final BlobContainerClient blobContainerClient = client.getBlobContainerClient(container);
            final BlobClient blobClient = blobContainerClient.getBlobClient(blob);
            final long totalSize;
            if (length == null) {
                totalSize = blobClient.getProperties().getBlobSize();
            } else {
                totalSize = position + length;
            }
            BlobAsyncClient blobAsyncClient = asyncClient().getBlobContainerAsyncClient(container).getBlobAsyncClient(blob);
            int maxReadRetries = service.getMaxReadRetries(clientName);
            return new AzureInputStream(blobAsyncClient, position, totalSize, (int) getReadChunkSize(), totalSize, maxReadRetries);
        });
    }

    public Map<String, BlobMetadata> listBlobsByPrefix(String keyPath, String prefix) throws IOException {
        final var blobsBuilder = new HashMap<String, BlobMetadata>();
        logger.trace(() ->
            new ParameterizedMessage("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix));
        try {
            final BlobServiceClient client = client();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobContainerClient containerClient = client.getBlobContainerClient(container);
                final BlobListDetails details = new BlobListDetails().setRetrieveMetadata(true);
                final ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
                    .setPrefix(keyPath + (prefix == null ? "" : prefix))
                    .setDetails(details);

                for (final BlobItem blobItem : containerClient.listBlobsByHierarchy("/", listBlobsOptions, null)) {
                    BlobItemProperties properties = blobItem.getProperties();
                    Boolean isPrefix = blobItem.isPrefix();
                    if (isPrefix != null && isPrefix) {
                        continue;
                    }
                    String blobName = blobItem.getName().substring(keyPath.length());

                    blobsBuilder.put(blobName,
                        new PlainBlobMetadata(blobName, properties.getContentLength()));
                }
            });
        } catch (Exception e) {
            throw new IOException("Unable to list blobs by prefix [" + prefix + "] for path " + keyPath, e);
        }
        return Map.copyOf(blobsBuilder);
    }

    public Map<String, BlobContainer> children(BlobPath path) throws IOException {
        final var childrenBuilder = new HashMap<String, BlobContainer>();
        final String keyPath = path.buildAsString();

        try {
            final BlobServiceClient client = client();
            SocketAccess.doPrivilegedVoidException(() -> {
                BlobContainerClient blobContainer = client.getBlobContainerClient(container);
                final ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
                listBlobsOptions.setPrefix(keyPath).setDetails(new BlobListDetails().setRetrieveMetadata(true));
                for (final BlobItem blobItem : blobContainer.listBlobsByHierarchy("/", listBlobsOptions, null)) {
                    Boolean isPrefix = blobItem.isPrefix();
                    if (isPrefix != null && isPrefix) {
                        String directoryName = blobItem.getName();
                        directoryName = directoryName.substring(keyPath.length());
                        if (directoryName.isEmpty()) {
                            continue;
                        }
                        // Remove trailing slash
                        directoryName = directoryName.substring(0, directoryName.length() - 1);
                        childrenBuilder.put(directoryName,
                            new AzureBlobContainer(BlobPath.cleanPath().add(blobItem.getName()), this));
                    }
                }
            });
        } catch (Exception e) {
            throw new IOException("Unable to provide children blob containers for " + path, e);
        }

        return Collections.unmodifiableMap(childrenBuilder);
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        assert inputStream.markSupported()
            : "Should not be used with non-mark supporting streams as their retry handling in the SDK is broken";
        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {})", blobName, blobSize));
        try {
            final BlobServiceClient client = client();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobClient blob = client.getBlobContainerClient(container)
                    .getBlobClient(blobName);

                ParallelTransferOptions parallelTransferOptions = getParallelTransferOptions();
                BlobParallelUploadOptions blobParallelUploadOptions =
                    new BlobParallelUploadOptions(inputStream, blobSize)
                        .setParallelTransferOptions(parallelTransferOptions);
                blob.uploadWithResponse(blobParallelUploadOptions, null, null);
            });
        } catch (final BlobStorageException e) {
            if (failIfAlreadyExists && e.getStatusCode() == HttpURLConnection.HTTP_CONFLICT &&
                BlobErrorCode.BLOB_ALREADY_EXISTS.equals(e.getErrorCode())) {
                throw new FileAlreadyExistsException(blobName, null, e.getMessage());
            }
            throw new IOException("Unable to write blob " + blobName, e);
        } catch (Exception e) {
            throw new IOException("Unable to write blob " + blobName, e);
        }

        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobName, blobSize));
    }

    private ParallelTransferOptions getParallelTransferOptions() {
        ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions();
        parallelTransferOptions.setBlockSizeLong(service.getUploadBlockSize())
            .setMaxSingleUploadSizeLong(service.getSizeThresholdForMultiBlockUpload())
            .setMaxConcurrency(service.getMaxUploadParallelism());
        return parallelTransferOptions;
    }

    private BlobServiceClient client() {
        return getAzureBlobServiceClientClient().getSyncClient();
    }

    private BlobServiceAsyncClient asyncClient() {
        return getAzureBlobServiceClientClient().getAsyncClient();
    }

    private AzureBlobServiceClient getAzureBlobServiceClientClient() {
        return service.client(clientName, locationMode, statsConsumer);
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

    private static class AzureInputStream extends StorageInputStream {
        private final BlobAsyncClient client;
        private final ByteBuffer buffer;
        private final int maxRetries;
        private final long firstReadOffset;
        private final int firstReadLength;

        private AzureInputStream(final BlobAsyncClient client,
                                 long rangeOffset,
                                 Long rangeLength,
                                 int chunkSize,
                                 long contentLength,
                                 int maxRetries) throws IOException {
            super(rangeOffset, rangeLength, chunkSize, contentLength);
            this.client = client;
            this.buffer = ByteBuffer.allocate(chunkSize);
            this.maxRetries = maxRetries;

            // Read eagerly the first chunk so we can throw early if the
            // blob doesn't exist
            this.firstReadOffset = rangeOffset;
            this.firstReadLength = (int) Math.min(chunkSize, contentLength - rangeOffset);
            executeRead(firstReadLength, rangeOffset);
        }

        @Override
        protected ByteBuffer dispatchRead(int readLength, long offset) throws IOException {
            // If the request is for the first chunk, don't download it again
            // Since we disabled marking in this InputStream, the offset only advances
            // and never goes back requesting the firstReadOffset again
            if (offset != firstReadOffset || readLength != firstReadLength) {
                executeRead(readLength, offset);
            }

            this.bufferSize = buffer.remaining();
            this.bufferStartOffset = offset;
            return buffer;
        }

        private void executeRead(long readLength, long offset) throws IOException {
            try {
                buffer.clear();
                final BlobRange range = new BlobRange(offset, readLength);
                DownloadRetryOptions downloadRetryOptions = new DownloadRetryOptions()
                    .setMaxRetryRequests(maxRetries);
                client.downloadWithResponse(range, downloadRetryOptions, null, false)
                    .flux()
                    .flatMap(ResponseBase::getValue)
                    .filter(Objects::nonNull)
                    .collect(() -> buffer, ByteBuffer::put)
                    .block();
            } catch (Exception e) {
                this.streamFaulted = true;
                this.lastError = new IOException(e);
                throw this.lastError;
            }

            buffer.flip();
        }

        @Override
        public synchronized void mark(int readlimit) {
            throwNotSupported();
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public synchronized void reset() {
            throwNotSupported();
        }

        @Override
        public synchronized long skip(long n) {
            return throwNotSupported();
        }

        private long throwNotSupported() {
            throw new UnsupportedOperationException("mark is not supported");
        }
    }

    private static class RequestStatsCollector {
        private final BiPredicate<String, URL> filter;
        private final Runnable onHttpRequest;

        private RequestStatsCollector(BiPredicate<String, URL> filter,
                                      Runnable onHttpRequest) {
            this.filter = filter;
            this.onHttpRequest = onHttpRequest;
        }

        static RequestStatsCollector create(BiPredicate<String, URL> filter,
                                            Runnable consumer) {
            return new RequestStatsCollector(filter, consumer);
        }

        private boolean shouldConsumeRequestInfo(String httpMethod, URL url) {
            return filter.test(httpMethod, url);
        }

        private void consumeHttpRequestInfo() {
            onHttpRequest.run();
        }
    }
}
