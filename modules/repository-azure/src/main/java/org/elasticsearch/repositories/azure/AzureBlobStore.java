/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import com.azure.core.http.rest.ResponseBase;
import com.azure.core.util.BinaryData;
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
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.options.BlockBlobSimpleUploadOptions;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.BlobContainerUtils;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;
import org.elasticsearch.repositories.blobstore.ChunkedBlobOutputStream;
import org.elasticsearch.rest.RestStatus;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.stream.StreamSupport;

import static org.elasticsearch.core.Strings.format;

public class AzureBlobStore implements BlobStore {
    private static final Logger logger = LogManager.getLogger(AzureBlobStore.class);
    private static final long DEFAULT_READ_CHUNK_SIZE = new ByteSizeValue(32, ByteSizeUnit.MB).getBytes();
    private static final int DEFAULT_UPLOAD_BUFFERS_SIZE = (int) new ByteSizeValue(64, ByteSizeUnit.KB).getBytes();

    private final AzureStorageService service;

    private final BigArrays bigArrays;

    private final String clientName;
    private final String container;
    private final LocationMode locationMode;
    private final ByteSizeValue maxSinglePartUploadSize;

    private final Stats stats = new Stats();
    private final BiConsumer<String, URL> statsConsumer;

    public AzureBlobStore(RepositoryMetadata metadata, AzureStorageService service, BigArrays bigArrays) {
        this.container = Repository.CONTAINER_SETTING.get(metadata.settings());
        this.clientName = Repository.CLIENT_NAME.get(metadata.settings());
        this.service = service;
        this.bigArrays = bigArrays;
        // locationMode is set per repository, not per client
        this.locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        this.maxSinglePartUploadSize = Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.get(metadata.settings());

        List<RequestStatsCollector> requestStatsCollectors = List.of(
            RequestStatsCollector.create((httpMethod, url) -> httpMethod.equals("HEAD"), stats.headOperations::incrementAndGet),
            RequestStatsCollector.create(
                (httpMethod, url) -> httpMethod.equals("GET") && isListRequest(httpMethod, url) == false,
                stats.getOperations::incrementAndGet
            ),
            RequestStatsCollector.create(this::isListRequest, stats.listOperations::incrementAndGet),
            RequestStatsCollector.create(this::isPutBlockRequest, stats.putBlockOperations::incrementAndGet),
            RequestStatsCollector.create(this::isPutBlockListRequest, stats.putBlockListOperations::incrementAndGet),
            RequestStatsCollector.create(
                // https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#uri-parameters
                // The only URI parameter allowed for put-blob operation is "timeout", but if a sas token is used,
                // it's possible that the URI parameters contain additional parameters unrelated to the upload type.
                (httpMethod, url) -> httpMethod.equals("PUT")
                    && isPutBlockRequest(httpMethod, url) == false
                    && isPutBlockListRequest(httpMethod, url) == false,
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
        return httpMethod.equals("GET") && url.getQuery() != null && url.getQuery().contains("comp=list");
    }

    // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block
    private boolean isPutBlockRequest(String httpMethod, URL url) {
        String queryParams = url.getQuery() == null ? "" : url.getQuery();
        return httpMethod.equals("PUT") && queryParams.contains("comp=block") && queryParams.contains("blockid=");
    }

    // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
    private boolean isPutBlockListRequest(String httpMethod, URL url) {
        String queryParams = url.getQuery() == null ? "" : url.getQuery();
        return httpMethod.equals("PUT") && queryParams.contains("comp=blocklist");
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
    public void close() {}

    public boolean blobExists(String blob) throws IOException {
        final BlobServiceClient client = client();

        try {
            Boolean blobExists = SocketAccess.doPrivilegedException(() -> {
                final BlobClient azureBlob = client.getBlobContainerClient(container).getBlobClient(blob);
                return azureBlob.exists();
            });
            return Boolean.TRUE.equals(blobExists);
        } catch (Exception e) {
            logger.trace("can not access [{}] in container {{}}: {}", blob, container, e.getMessage());
            throw new IOException("Unable to check if blob " + blob + " exists", e);
        }
    }

    // number of concurrent blob delete requests to use while bulk deleting
    private static final int CONCURRENT_DELETES = 100;

    public DeleteResult deleteBlobDirectory(String path) throws IOException {
        final AtomicInteger blobsDeleted = new AtomicInteger(0);
        final AtomicLong bytesDeleted = new AtomicLong(0);

        SocketAccess.doPrivilegedVoidException(() -> {
            final BlobContainerAsyncClient blobContainerAsyncClient = asyncClient().getBlobContainerAsyncClient(container);
            final ListBlobsOptions options = new ListBlobsOptions().setPrefix(path)
                .setDetails(new BlobListDetails().setRetrieveMetadata(true));
            try {
                blobContainerAsyncClient.listBlobs(options, null).flatMap(blobItem -> {
                    if (blobItem.isPrefix() != null && blobItem.isPrefix()) {
                        return Mono.empty();
                    } else {
                        final String blobName = blobItem.getName();
                        BlobAsyncClient blobAsyncClient = blobContainerAsyncClient.getBlobAsyncClient(blobName);
                        final Mono<Void> deleteTask = getDeleteTask(blobName, blobAsyncClient);
                        bytesDeleted.addAndGet(blobItem.getProperties().getContentLength());
                        blobsDeleted.incrementAndGet();
                        return deleteTask;
                    }
                }, CONCURRENT_DELETES).then().block();
            } catch (Exception e) {
                filterDeleteExceptionsAndRethrow(e, new IOException("Deleting directory [" + path + "] failed"));
            }
        });

        return new DeleteResult(blobsDeleted.get(), bytesDeleted.get());
    }

    private static void filterDeleteExceptionsAndRethrow(Exception e, IOException exception) throws IOException {
        int suppressedCount = 0;
        for (Throwable suppressed : e.getSuppressed()) {
            // We're only interested about the blob deletion exceptions and not in the reactor internals exceptions
            if (suppressed instanceof IOException) {
                exception.addSuppressed(suppressed);
                suppressedCount++;
                if (suppressedCount > 10) {
                    break;
                }
            }
        }
        throw exception;
    }

    void deleteBlobs(Iterator<String> blobs) throws IOException {
        if (blobs.hasNext() == false) {
            return;
        }

        BlobServiceAsyncClient asyncClient = asyncClient();
        SocketAccess.doPrivilegedVoidException(() -> {
            final BlobContainerAsyncClient blobContainerClient = asyncClient.getBlobContainerAsyncClient(container);
            try {
                Flux.fromStream(StreamSupport.stream(Spliterators.spliteratorUnknownSize(blobs, Spliterator.ORDERED), false))
                    .flatMap(blob -> getDeleteTask(blob, blobContainerClient.getBlobAsyncClient(blob)), CONCURRENT_DELETES)
                    .then()
                    .block();
            } catch (Exception e) {
                filterDeleteExceptionsAndRethrow(e, new IOException("Unable to delete blobs"));
            }
        });
    }

    private static Mono<Void> getDeleteTask(String blobName, BlobAsyncClient blobAsyncClient) {
        return blobAsyncClient.delete()
            // Ignore not found blobs, as it's possible that due to network errors a request
            // for an already deleted blob is retried, causing an error.
            .onErrorResume(
                e -> e instanceof BlobStorageException blobStorageException && blobStorageException.getStatusCode() == 404,
                throwable -> Mono.empty()
            )
            .onErrorMap(throwable -> new IOException("Error deleting blob " + blobName, throwable));
    }

    public InputStream getInputStream(String blob, long position, final @Nullable Long length) throws IOException {
        logger.trace(() -> format("reading container [%s], blob [%s]", container, blob));
        final AzureBlobServiceClient azureBlobServiceClient = getAzureBlobServiceClientClient();
        final BlobServiceClient syncClient = azureBlobServiceClient.getSyncClient();
        final BlobServiceAsyncClient asyncClient = azureBlobServiceClient.getAsyncClient();

        return SocketAccess.doPrivilegedException(() -> {
            final BlobContainerClient blobContainerClient = syncClient.getBlobContainerClient(container);
            final BlobClient blobClient = blobContainerClient.getBlobClient(blob);
            final long totalSize;
            if (length == null) {
                totalSize = blobClient.getProperties().getBlobSize();
            } else {
                totalSize = position + length;
            }
            BlobAsyncClient blobAsyncClient = asyncClient.getBlobContainerAsyncClient(container).getBlobAsyncClient(blob);
            int maxReadRetries = service.getMaxReadRetries(clientName);
            return new AzureInputStream(
                blobAsyncClient,
                position,
                length == null ? totalSize : length,
                totalSize,
                maxReadRetries,
                azureBlobServiceClient.getAllocator()
            );
        });
    }

    public Map<String, BlobMetadata> listBlobsByPrefix(String keyPath, String prefix) throws IOException {
        final var blobsBuilder = new HashMap<String, BlobMetadata>();
        logger.trace(() -> format("listing container [%s], keyPath [%s], prefix [%s]", container, keyPath, prefix));
        try {
            final BlobServiceClient client = client();
            SocketAccess.doPrivilegedVoidException(() -> {
                final BlobContainerClient containerClient = client.getBlobContainerClient(container);
                final BlobListDetails details = new BlobListDetails().setRetrieveMetadata(true);
                final ListBlobsOptions listBlobsOptions = new ListBlobsOptions().setPrefix(keyPath + (prefix == null ? "" : prefix))
                    .setDetails(details);

                for (final BlobItem blobItem : containerClient.listBlobsByHierarchy("/", listBlobsOptions, null)) {
                    BlobItemProperties properties = blobItem.getProperties();
                    Boolean isPrefix = blobItem.isPrefix();
                    if (isPrefix != null && isPrefix) {
                        continue;
                    }
                    String blobName = blobItem.getName().substring(keyPath.length());

                    blobsBuilder.put(blobName, new BlobMetadata(blobName, properties.getContentLength()));
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
                        childrenBuilder.put(directoryName, new AzureBlobContainer(BlobPath.EMPTY.add(blobItem.getName()), this));
                    }
                }
            });
        } catch (Exception e) {
            throw new IOException("Unable to provide children blob containers for " + path, e);
        }

        return Collections.unmodifiableMap(childrenBuilder);
    }

    public void writeBlob(String blobName, BytesReference bytes, boolean failIfAlreadyExists) {
        Flux<ByteBuffer> byteBufferFlux = Flux.fromArray(BytesReference.toByteBuffers(bytes));
        executeSingleUpload(blobName, byteBufferFlux, bytes.length(), failIfAlreadyExists);
    }

    public void writeBlob(String blobName, boolean failIfAlreadyExists, CheckedConsumer<OutputStream, IOException> writer)
        throws IOException {
        final BlockBlobAsyncClient blockBlobAsyncClient = asyncClient().getBlobContainerAsyncClient(container)
            .getBlobAsyncClient(blobName)
            .getBlockBlobAsyncClient();
        try (ChunkedBlobOutputStream<String> out = new ChunkedBlobOutputStream<>(bigArrays, getUploadBlockSize()) {

            @Override
            protected void flushBuffer() {
                if (buffer.size() == 0) {
                    return;
                }
                final String blockId = makeMultipartBlockId();
                SocketAccess.doPrivilegedVoidException(
                    () -> blockBlobAsyncClient.stageBlock(
                        blockId,
                        Flux.fromArray(BytesReference.toByteBuffers(buffer.bytes())),
                        buffer.size()
                    ).block()
                );
                finishPart(blockId);
            }

            @Override
            protected void onCompletion() {
                if (flushedBytes == 0L) {
                    writeBlob(blobName, buffer.bytes(), failIfAlreadyExists);
                } else {
                    flushBuffer();
                    SocketAccess.doPrivilegedVoidException(
                        () -> blockBlobAsyncClient.commitBlockList(parts, failIfAlreadyExists == false).block()
                    );
                }
            }

            @Override
            protected void onFailure() {
                // Nothing to do here, already uploaded blocks will be GCed by Azure after a week.
                // see https://docs.microsoft.com/en-us/rest/api/storageservices/put-block#remarks
            }
        }) {
            writer.accept(out);
            out.markSuccess();
        }
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        assert inputStream.markSupported()
            : "Should not be used with non-mark supporting streams as their retry handling in the SDK is broken";
        logger.trace(() -> format("writeBlob(%s, stream, %s)", blobName, blobSize));
        try {
            if (blobSize <= getLargeBlobThresholdInBytes()) {
                final Flux<ByteBuffer> byteBufferFlux = convertStreamToByteBuffer(inputStream, blobSize, DEFAULT_UPLOAD_BUFFERS_SIZE);
                executeSingleUpload(blobName, byteBufferFlux, blobSize, failIfAlreadyExists);
            } else {
                executeMultipartUpload(blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        } catch (final BlobStorageException e) {
            if (failIfAlreadyExists
                && e.getStatusCode() == HttpURLConnection.HTTP_CONFLICT
                && BlobErrorCode.BLOB_ALREADY_EXISTS.equals(e.getErrorCode())) {
                throw new FileAlreadyExistsException(blobName, null, e.getMessage());
            }
            throw new IOException("Unable to write blob " + blobName, e);
        } catch (Exception e) {
            throw new IOException("Unable to write blob " + blobName, e);
        }

        logger.trace(() -> format("writeBlob(%s, stream, %s) - done", blobName, blobSize));
    }

    private void executeSingleUpload(String blobName, Flux<ByteBuffer> byteBufferFlux, long blobSize, boolean failIfAlreadyExists) {
        SocketAccess.doPrivilegedVoidException(() -> {
            final BlobServiceAsyncClient asyncClient = asyncClient();

            final BlobAsyncClient blobAsyncClient = asyncClient.getBlobContainerAsyncClient(container).getBlobAsyncClient(blobName);
            final BlockBlobAsyncClient blockBlobAsyncClient = blobAsyncClient.getBlockBlobAsyncClient();

            final BlockBlobSimpleUploadOptions options = new BlockBlobSimpleUploadOptions(byteBufferFlux, blobSize);
            BlobRequestConditions requestConditions = new BlobRequestConditions();
            if (failIfAlreadyExists) {
                requestConditions.setIfNoneMatch("*");
            }
            options.setRequestConditions(requestConditions);
            blockBlobAsyncClient.uploadWithResponse(options).block();
        });
    }

    private void executeMultipartUpload(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
        SocketAccess.doPrivilegedVoidException(() -> {
            final BlobServiceAsyncClient asyncClient = asyncClient();
            final BlobAsyncClient blobAsyncClient = asyncClient.getBlobContainerAsyncClient(container).getBlobAsyncClient(blobName);
            final BlockBlobAsyncClient blockBlobAsyncClient = blobAsyncClient.getBlockBlobAsyncClient();

            final long partSize = getUploadBlockSize();
            final Tuple<Long, Long> multiParts = numberOfMultiparts(blobSize, partSize);
            final int nbParts = multiParts.v1().intValue();
            final long lastPartSize = multiParts.v2();
            assert blobSize == (((nbParts - 1) * partSize) + lastPartSize) : "blobSize does not match multipart sizes";

            final List<String> blockIds = new ArrayList<>(nbParts);
            for (int i = 0; i < nbParts; i++) {
                final long length = i < nbParts - 1 ? partSize : lastPartSize;
                Flux<ByteBuffer> byteBufferFlux = convertStreamToByteBuffer(inputStream, length, DEFAULT_UPLOAD_BUFFERS_SIZE);

                final String blockId = makeMultipartBlockId();
                blockBlobAsyncClient.stageBlock(blockId, byteBufferFlux, length).block();
                blockIds.add(blockId);
            }

            blockBlobAsyncClient.commitBlockList(blockIds, failIfAlreadyExists == false).block();
        });
    }

    private static final Base64.Encoder base64Encoder = Base64.getEncoder().withoutPadding();
    private static final Base64.Decoder base64UrlDecoder = Base64.getUrlDecoder();

    private String makeMultipartBlockId() {
        return base64Encoder.encodeToString(base64UrlDecoder.decode(UUIDs.base64UUID()));
    }

    /**
     * Converts the provided input stream into a Flux of ByteBuffer. To avoid having large amounts of outstanding
     * memory this Flux reads the InputStream into ByteBuffers of {@code chunkSize} size.
     * @param delegate the InputStream to convert
     * @param length the InputStream length
     * @param chunkSize the chunk size in bytes
     * @return a Flux of ByteBuffers
     */
    private Flux<ByteBuffer> convertStreamToByteBuffer(InputStream delegate, long length, int chunkSize) {
        assert delegate.markSupported() : "An InputStream with mark support was expected";
        // We need to introduce a read barrier in order to provide visibility for the underlying
        // input stream state as the input stream can be read from different threads.
        final InputStream inputStream = new FilterInputStream(delegate) {
            @Override
            public synchronized int read(byte[] b, int off, int len) throws IOException {
                return super.read(b, off, len);
            }

            @Override
            public synchronized int read() throws IOException {
                return super.read();
            }
        };
        // We need to mark the InputStream as it's possible that we need to retry for the same chunk
        inputStream.mark(Integer.MAX_VALUE);
        return Flux.defer(() -> {
            final AtomicLong currentTotalLength = new AtomicLong(0);
            try {
                inputStream.reset();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // This flux is subscribed by a downstream operator that finally queues the
            // buffers into netty output queue. Sadly we are not able to get a signal once
            // the buffer has been flushed, so we have to allocate those and let the GC to
            // reclaim them (see MonoSendMany). Additionally, that very same operator requests
            // 128 elements (that's hardcoded) once it's subscribed (later on, it requests
            // by 64 elements), that's why we provide 64kb buffers.

            // length is at most 100MB so it's safe to cast back to an integer in this case
            final int parts = (int) length / chunkSize;
            final long remaining = length % chunkSize;
            return Flux.range(0, remaining == 0 ? parts : parts + 1).map(i -> i * chunkSize).concatMap(pos -> Mono.fromCallable(() -> {
                long count = pos + chunkSize > length ? length - pos : chunkSize;
                int numOfBytesRead = 0;
                int offset = 0;
                int len = (int) count;
                final byte[] buffer = new byte[len];
                while (numOfBytesRead != -1 && offset < count) {
                    numOfBytesRead = inputStream.read(buffer, offset, len);
                    offset += numOfBytesRead;
                    len -= numOfBytesRead;
                    if (numOfBytesRead != -1) {
                        currentTotalLength.addAndGet(numOfBytesRead);
                    }
                }
                if (numOfBytesRead == -1 && currentTotalLength.get() < length) {
                    throw new IllegalStateException(
                        "InputStream provided" + currentTotalLength + " bytes, less than the expected" + length + " bytes"
                    );
                }
                return ByteBuffer.wrap(buffer);
            })).doOnComplete(() -> {
                if (currentTotalLength.get() > length) {
                    throw new IllegalStateException(
                        "Read more data than was requested. Size of data read: "
                            + currentTotalLength.get()
                            + "."
                            + " Size of data requested: "
                            + length
                    );
                }
            });
        }).subscribeOn(Schedulers.elastic()); // We need to subscribe on a different scheduler to avoid blocking the io threads when
                                              // we read the input stream (i.e. when it's rate limited)
    }

    /**
     * Returns the number parts of size of {@code partSize} needed to reach {@code totalSize},
     * along with the size of the last (or unique) part.
     *
     * @param totalSize the total size
     * @param partSize  the part size
     * @return a {@link Tuple} containing the number of parts to fill {@code totalSize} and
     * the size of the last part
     */
    static Tuple<Long, Long> numberOfMultiparts(final long totalSize, final long partSize) {
        if (partSize <= 0) {
            throw new IllegalArgumentException("Part size must be greater than zero");
        }

        if ((totalSize == 0L) || (totalSize <= partSize)) {
            return Tuple.tuple(1L, totalSize);
        }

        final long parts = totalSize / partSize;
        final long remaining = totalSize % partSize;

        if (remaining == 0) {
            return Tuple.tuple(parts, partSize);
        } else {
            return Tuple.tuple(parts + 1, remaining);
        }
    }

    long getLargeBlobThresholdInBytes() {
        return maxSinglePartUploadSize.getBytes();
    }

    long getUploadBlockSize() {
        return service.getUploadBlockSize();
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
            return Map.of(
                "GetBlob",
                getOperations.get(),
                "ListBlobs",
                listOperations.get(),
                "GetBlobProperties",
                headOperations.get(),
                "PutBlob",
                putOperations.get(),
                "PutBlock",
                putBlockOperations.get(),
                "PutBlockList",
                putBlockListOperations.get()
            );
        }
    }

    private static class AzureInputStream extends InputStream {
        private final CancellableRateLimitedFluxIterator<ByteBuf> cancellableRateLimitedFluxIterator;
        private ByteBuf byteBuf;
        private boolean closed;
        private final ByteBufAllocator allocator;

        private AzureInputStream(
            final BlobAsyncClient client,
            long rangeOffset,
            long rangeLength,
            long contentLength,
            int maxRetries,
            ByteBufAllocator allocator
        ) throws IOException {
            rangeLength = Math.min(rangeLength, contentLength - rangeOffset);
            final BlobRange range = new BlobRange(rangeOffset, rangeLength);
            DownloadRetryOptions downloadRetryOptions = new DownloadRetryOptions().setMaxRetryRequests(maxRetries);
            Flux<ByteBuf> byteBufFlux = client.downloadWithResponse(range, downloadRetryOptions, null, false)
                .flux()
                .concatMap(ResponseBase::getValue) // it's important to use concatMap, since flatMap doesn't provide ordering
                                                   // guarantees and that's not fun to debug :(
                .filter(Objects::nonNull)
                .map(this::copyBuffer); // Sadly we have to copy the buffers since the memory is released after the flux execution
                                        // ends and we need that the byte buffer outlives that lifecycle. Since the SDK provides an
                                        // ByteBuffer instead of a ByteBuf we cannot just increase the ref count and release the
                                        // memory later on.
            this.allocator = allocator;

            // On the transport layer we read the recv buffer in 64kb chunks, but later on those buffers are
            // split into 8kb chunks (see HttpObjectDecoder), so we request upstream the equivalent to 64kb. (i.e. 8 elements per batch *
            // 8kb)
            this.cancellableRateLimitedFluxIterator = new CancellableRateLimitedFluxIterator<>(8, ReferenceCountUtil::safeRelease);
            // Read eagerly the first chunk so we can throw early if the
            // blob doesn't exist
            byteBufFlux.subscribe(cancellableRateLimitedFluxIterator);
            getNextByteBuf();
        }

        private ByteBuf copyBuffer(ByteBuffer buffer) {
            ByteBuf byteBuffer = allocator.heapBuffer(buffer.remaining(), buffer.remaining());
            byteBuffer.writeBytes(buffer);
            return byteBuffer;
        }

        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            var bytesRead = read(b, 0, 1);

            if (bytesRead > 1) {
                throw new IOException("Stream returned more data than requested");
            }

            if (bytesRead == 1) {
                return b[0] & 0xFF;
            } else if (bytesRead == 0) {
                throw new IOException("Stream returned unexpected number of bytes");
            } else {
                return -1;
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            }

            ByteBuf buffer = getNextByteBuf();
            if (buffer == null || buffer.readableBytes() == 0) {
                releaseByteBuf(buffer);
                return -1;
            }

            int totalBytesRead = 0;
            while (buffer != null && totalBytesRead < len) {
                int toRead = Math.min(len - totalBytesRead, buffer.readableBytes());
                buffer.readBytes(b, off + totalBytesRead, toRead);
                totalBytesRead += toRead;
                if (buffer.readableBytes() == 0) {
                    releaseByteBuf(buffer);
                    buffer = getNextByteBuf();
                }
            }

            return totalBytesRead;
        }

        @Override
        public void close() {
            if (closed == false) {
                cancellableRateLimitedFluxIterator.cancel();
                closed = true;
                releaseByteBuf(byteBuf);
            }
        }

        private void releaseByteBuf(ByteBuf buf) {
            ReferenceCountUtil.safeRelease(buf);
            this.byteBuf = null;
        }

        @Nullable
        private ByteBuf getNextByteBuf() throws IOException {
            try {
                if (byteBuf == null && cancellableRateLimitedFluxIterator.hasNext() == false) {
                    return null;
                }

                if (byteBuf != null) {
                    return byteBuf;
                }

                byteBuf = cancellableRateLimitedFluxIterator.next();
                return byteBuf;
            } catch (Exception e) {
                throw new IOException("Unable to read blob", e.getCause());
            }
        }
    }

    private static class RequestStatsCollector {
        private final BiPredicate<String, URL> filter;
        private final Runnable onHttpRequest;

        private RequestStatsCollector(BiPredicate<String, URL> filter, Runnable onHttpRequest) {
            this.filter = filter;
            this.onHttpRequest = onHttpRequest;
        }

        static RequestStatsCollector create(BiPredicate<String, URL> filter, Runnable consumer) {
            return new RequestStatsCollector(filter, consumer);
        }

        private boolean shouldConsumeRequestInfo(String httpMethod, URL url) {
            return filter.test(httpMethod, url);
        }

        private void consumeHttpRequestInfo() {
            onHttpRequest.run();
        }
    }

    OptionalBytesReference getRegister(String blobPath, String containerPath, String blobKey) {
        try {
            return SocketAccess.doPrivilegedException(
                () -> OptionalBytesReference.of(
                    downloadRegisterBlob(
                        containerPath,
                        blobKey,
                        getAzureBlobServiceClientClient().getSyncClient().getBlobContainerClient(container).getBlobClient(blobPath),
                        null
                    )
                )
            );
        } catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof BlobStorageException blobStorageException
                && blobStorageException.getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
                return OptionalBytesReference.MISSING;
            }
            throw e;
        }
    }

    OptionalBytesReference compareAndExchangeRegister(
        String blobPath,
        String containerPath,
        String blobKey,
        BytesReference expected,
        BytesReference updated
    ) {
        BlobContainerUtils.ensureValidRegisterContent(updated);
        try {
            return SocketAccess.doPrivilegedException(
                () -> OptionalBytesReference.of(
                    innerCompareAndExchangeRegister(
                        containerPath,
                        blobKey,
                        getAzureBlobServiceClientClient().getSyncClient().getBlobContainerClient(container).getBlobClient(blobPath),
                        expected,
                        updated
                    )
                )
            );
        } catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof BlobStorageException blobStorageException) {
                if (blobStorageException.getStatusCode() == RestStatus.PRECONDITION_FAILED.getStatus()
                    || blobStorageException.getStatusCode() == RestStatus.CONFLICT.getStatus()) {
                    return OptionalBytesReference.MISSING;
                }
            }
            throw e;
        }
    }

    private static BytesReference innerCompareAndExchangeRegister(
        String containerPath,
        String blobKey,
        BlobClient blobClient,
        BytesReference expected,
        BytesReference updated
    ) throws IOException {
        if (blobClient.exists()) {
            final var leaseClient = new BlobLeaseClientBuilder().blobClient(blobClient).buildClient();
            final var leaseId = leaseClient.acquireLease(60);
            try {
                final BytesReference currentValue = downloadRegisterBlob(
                    containerPath,
                    blobKey,
                    blobClient,
                    new BlobRequestConditions().setLeaseId(leaseId)
                );
                if (currentValue.equals(expected)) {
                    uploadRegisterBlob(updated, blobClient, new BlobRequestConditions().setLeaseId(leaseId));
                }
                return currentValue;
            } finally {
                leaseClient.releaseLease();
            }
        } else {
            if (expected.length() == 0) {
                uploadRegisterBlob(updated, blobClient, new BlobRequestConditions().setIfNoneMatch("*"));
            }
            return BytesArray.EMPTY;
        }
    }

    private static BytesReference downloadRegisterBlob(
        String containerPath,
        String blobKey,
        BlobClient blobClient,
        BlobRequestConditions blobRequestConditions
    ) throws IOException {
        return BlobContainerUtils.getRegisterUsingConsistentRead(
            blobClient.downloadContentWithResponse(new DownloadRetryOptions().setMaxRetryRequests(0), blobRequestConditions, null, null)
                .getValue()
                .toStream(),
            containerPath,
            blobKey
        );
    }

    private static void uploadRegisterBlob(BytesReference blobContents, BlobClient blobClient, BlobRequestConditions requestConditions)
        throws IOException {
        blobClient.uploadWithResponse(
            new BlobParallelUploadOptions(BinaryData.fromStream(blobContents.streamInput(), (long) blobContents.length()))
                .setRequestConditions(requestConditions),
            null,
            null
        );
    }

}
