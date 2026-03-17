/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.cloud.BaseServiceException;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.BlobContainerUtils;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.ByteArrayInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.net.HttpURLConnection.HTTP_GONE;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.rest.RestStatus.TOO_MANY_REQUESTS;

class GoogleCloudStorageBlobStore implements BlobStore {

    /**
     * see {@link com.google.cloud.storage.BaseStorageWriteChannel#chunkSize}
     */
    static final int SDK_DEFAULT_CHUNK_SIZE = Math.toIntExact(ByteSizeValue.ofMb(16).getBytes());

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageBlobStore.class);

    // The recommended maximum size of a blob that should be uploaded in a single
    // request. Larger files should be uploaded over multiple requests (this is
    // called "resumable upload")
    // https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
    public static final int LARGE_BLOB_THRESHOLD_BYTE_SIZE;

    // MAX deletes per batch is 100 https://docs.cloud.google.com/storage/docs/batch#overview
    public static final int MAX_DELETES_PER_BATCH = 100;

    static {
        final String key = "es.repository_gcs.large_blob_threshold_byte_size";
        final String largeBlobThresholdByteSizeProperty = System.getProperty(key);
        if (largeBlobThresholdByteSizeProperty == null) {
            LARGE_BLOB_THRESHOLD_BYTE_SIZE = Math.toIntExact(ByteSizeValue.of(5, ByteSizeUnit.MB).getBytes());
        } else {
            final int largeBlobThresholdByteSize;
            try {
                largeBlobThresholdByteSize = Integer.parseInt(largeBlobThresholdByteSizeProperty);
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("failed to parse " + key + " having value [" + largeBlobThresholdByteSizeProperty + "]");
            }
            if (largeBlobThresholdByteSize <= 0) {
                throw new IllegalArgumentException(key + " must be positive but was [" + largeBlobThresholdByteSizeProperty + "]");
            }
            LARGE_BLOB_THRESHOLD_BYTE_SIZE = largeBlobThresholdByteSize;
        }
    }

    @Nullable // for cluster level object store in MP
    private final ProjectId projectId;
    protected final String bucketName;
    private final String clientName;
    private final String repositoryName;
    private final GoogleCloudStorageService storageService;
    private final GcsRepositoryStatsCollector statsCollector;
    private final int bufferSize;
    private final BigArrays bigArrays;
    private final BackoffPolicy casBackoffPolicy;
    private volatile boolean closed = false;

    GoogleCloudStorageBlobStore(
        ProjectId projectId,
        String bucketName,
        String clientName,
        String repositoryName,
        GoogleCloudStorageService storageService,
        BigArrays bigArrays,
        int bufferSize,
        BackoffPolicy casBackoffPolicy,
        GcsRepositoryStatsCollector statsCollector
    ) {
        this.projectId = projectId;
        this.bucketName = bucketName;
        this.clientName = clientName;
        this.repositoryName = repositoryName;
        this.storageService = storageService;
        this.bigArrays = bigArrays;
        this.statsCollector = statsCollector;
        this.bufferSize = bufferSize;
        this.casBackoffPolicy = casBackoffPolicy;
    }

    /**
     * @throws org.apache.lucene.store.AlreadyClosedException if the blob store is closed
     */
    MeteredStorage client() throws IOException {
        if (closed) {
            throw new AlreadyClosedException("blob store for repository " + repositoryName + "is closed");
        }
        return storageService.client(projectId, clientName, repositoryName, statsCollector);
    }

    int getMaxRetries() {
        return storageService.clientSettings(projectId, clientName).getMaxRetries();
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new GoogleCloudStorageBlobContainer(path, this);
    }

    @Override
    public void close() {
        closed = true;
        storageService.closeRepositoryClients(projectId, repositoryName);
    }

    /**
     * List blobs in the specific bucket under the specified path. The path root is removed.
     *
     * @param purpose the operation purpose
     * @param path base path of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetadata> listBlobs(OperationPurpose purpose, String path) throws IOException {
        return listBlobsByPrefix(purpose, path, "");
    }

    /**
     * List all blobs in the specific bucket with names prefixed
     *
     * @param purpose the operation purpose
     * @param path
     *            base path of the blobs to list. This path is removed from the
     *            names of the blobs returned.
     * @param prefix prefix of the blobs to list.
     * @return a map of blob names and their metadata.
     */
    Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String path, String prefix) throws IOException {
        final String pathPrefix = buildKey(path, prefix);
        final Map<String, BlobMetadata> mapBuilder = new HashMap<>();
        client().meteredList(purpose, bucketName, BlobListOption.currentDirectory(), BlobListOption.prefix(pathPrefix))
            .iterateAll()
            .forEach(blob -> {
                assert blob.getName().startsWith(path);
                if (blob.isDirectory() == false) {
                    final String suffixName = blob.getName().substring(path.length());
                    mapBuilder.put(suffixName, new BlobMetadata(suffixName, blob.getSize()));
                }
            });
        return Map.copyOf(mapBuilder);
    }

    Map<String, BlobContainer> listChildren(OperationPurpose purpose, BlobPath path) throws IOException {
        final String pathStr = path.buildAsString();
        final Map<String, BlobContainer> mapBuilder = new HashMap<>();
        client().meteredList(purpose, bucketName, BlobListOption.currentDirectory(), BlobListOption.prefix(pathStr))
            .iterateAll()
            .forEach(blob -> {
                if (blob.isDirectory()) {
                    assert blob.getName().startsWith(pathStr);
                    assert blob.getName().endsWith("/");
                    // Strip path prefix and trailing slash
                    final String suffixName = blob.getName().substring(pathStr.length(), blob.getName().length() - 1);
                    if (suffixName.isEmpty() == false) {
                        mapBuilder.put(suffixName, new GoogleCloudStorageBlobContainer(path.add(suffixName), this));
                    }
                }
            });
        return Map.copyOf(mapBuilder);
    }

    /**
     * Returns true if the blob exists in the specific bucket
     *
     * @param purpose the operation purpose
     * @param blobName name of the blob
     * @return true iff the blob exists
     */
    boolean blobExists(OperationPurpose purpose, String blobName) throws IOException {
        final BlobId blobId = BlobId.of(bucketName, blobName);
        final Blob blob = client().meteredGet(purpose, blobId);
        return blob != null;
    }

    /**
     * Returns an {@link java.io.InputStream} for the given blob name
     *
     * @param purpose the operation purpose
     * @param blobName name of the blob
     * @return the InputStream used to read the blob's content
     */
    InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
        return new GoogleCloudStorageRetryingInputStream(this, purpose, BlobId.of(bucketName, blobName));
    }

    /**
     * Returns an {@link java.io.InputStream} for the given blob's position and length
     *
     * @param purpose the operation purpose
     * @param blobName name of the blob
     * @param position starting position to read from
     * @param length length of bytes to read
     * @return the InputStream used to read the blob's content
     */
    InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
        if (position < 0L) {
            throw new IllegalArgumentException("position must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        } else {
            return new GoogleCloudStorageRetryingInputStream(
                this,
                purpose,
                BlobId.of(bucketName, blobName),
                position,
                Math.addExact(position, length - 1)
            );
        }
    }

    /**
     * Writes a blob in the specific bucket
     * @param purpose the operation purpose
     * @param bytes       content of the blob to be written
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     */
    void writeBlob(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        if (bytes.length() > getLargeBlobThresholdInBytes()) {
            // Compute md5 here so #writeBlobResumable forces the integrity check on the resumable upload.
            // This is needed since we rely on atomic write behavior when writing BytesReferences in BlobStoreRepository which is not
            // guaranteed for resumable uploads.
            final String md5 = Base64.getEncoder().encodeToString(MessageDigests.digest(bytes, MessageDigests.md5()));
            writeBlobResumable(
                purpose,
                BlobInfo.newBuilder(bucketName, blobName).setMd5(md5).build(),
                bytes.streamInput(),
                bytes.length(),
                failIfAlreadyExists
            );
        } else {
            final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, blobName).build();
            if (bytes.hasArray()) {
                writeBlobMultipart(purpose, blobInfo, bytes.array(), bytes.arrayOffset(), bytes.length(), failIfAlreadyExists);
            } else {
                writeBlob(purpose, bytes.streamInput(), bytes.length(), failIfAlreadyExists, blobInfo);
            }
        }
    }

    /**
     * Writes a blob in the specific bucket
     * @param purpose     the operation purpose
     * @param inputStream content of the blob to be written
     * @param blobSize    expected size of the blob to be written
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     */
    void writeBlob(OperationPurpose purpose, String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws IOException {
        writeBlob(purpose, inputStream, blobSize, failIfAlreadyExists, BlobInfo.newBuilder(bucketName, blobName).build());
    }

    private void writeBlob(OperationPurpose purpose, InputStream inputStream, long blobSize, boolean failIfAlreadyExists, BlobInfo blobInfo)
        throws IOException {
        if (blobSize > getLargeBlobThresholdInBytes()) {
            writeBlobResumable(purpose, blobInfo, inputStream, blobSize, failIfAlreadyExists);
        } else {
            final byte[] buffer = new byte[Math.toIntExact(blobSize)];
            Streams.readFully(inputStream, buffer);
            writeBlobMultipart(purpose, blobInfo, buffer, 0, Math.toIntExact(blobSize), failIfAlreadyExists);
        }
    }

    // non-static, package private for testing
    long getLargeBlobThresholdInBytes() {
        return LARGE_BLOB_THRESHOLD_BYTE_SIZE;
    }

    // possible options for #writeBlobResumable uploads
    private static final Storage.BlobWriteOption[] NO_OVERWRITE_NO_MD5 = { Storage.BlobWriteOption.doesNotExist() };
    private static final Storage.BlobWriteOption[] OVERWRITE_NO_MD5 = new Storage.BlobWriteOption[0];
    private static final Storage.BlobWriteOption[] NO_OVERWRITE_CHECK_MD5 = {
        Storage.BlobWriteOption.doesNotExist(),
        Storage.BlobWriteOption.md5Match() };
    private static final Storage.BlobWriteOption[] OVERWRITE_CHECK_MD5 = { Storage.BlobWriteOption.md5Match() };

    void writeBlob(
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, blobName).build();
        final Storage.BlobWriteOption[] writeOptions = failIfAlreadyExists ? NO_OVERWRITE_NO_MD5 : OVERWRITE_NO_MD5;

        StorageException storageException = null;

        for (int retry = 0; retry < 3; ++retry) {
            // we start out by buffering the write to a buffer, if it exceeds the large blob threshold we start a resumable upload, flush
            // the buffer to it and keep writing to the resumable upload. If we never exceed the large blob threshold we just write the
            // buffer via a standard blob write
            try (ReleasableBytesStreamOutput buffer = new ReleasableBytesStreamOutput(bigArrays)) {
                final AtomicReference<WriteChannel> channelRef = new AtomicReference<>();
                writer.accept(new OutputStream() {

                    private OutputStream resumableStream;

                    @Override
                    public void write(int b) throws IOException {
                        if (resumableStream != null) {
                            resumableStream.write(b);
                        } else {
                            if (buffer.size() + 1 > getLargeBlobThresholdInBytes()) {
                                initResumableStream();
                                resumableStream.write(b);
                            } else {
                                buffer.write(b);
                            }
                        }
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        if (resumableStream != null) {
                            resumableStream.write(b, off, len);
                        } else {
                            if (buffer.size() + len > getLargeBlobThresholdInBytes()) {
                                initResumableStream();
                                resumableStream.write(b, off, len);
                            } else {
                                buffer.write(b, off, len);
                            }
                        }
                    }

                    private void initResumableStream() throws IOException {
                        final var writeChannel = client().meteredWriter(purpose, blobInfo, writeOptions);
                        channelRef.set(writeChannel);
                        resumableStream = new FilterOutputStream(Channels.newOutputStream(new WritableBlobChannel(writeChannel))) {
                            @Override
                            public void write(byte[] b, int off, int len) throws IOException {
                                int written = 0;
                                while (written < len) {
                                    final int toWrite = Math.min(len - written, SDK_DEFAULT_CHUNK_SIZE);
                                    out.write(b, off + written, toWrite);
                                    written += toWrite;
                                }
                            }
                        };
                        buffer.bytes().writeTo(resumableStream);
                        buffer.close();
                    }
                });
                final WritableByteChannel writeChannel = channelRef.get();
                if (writeChannel != null) {
                    writeChannel.close();
                } else {
                    writeBlob(purpose, blobName, buffer.bytes(), failIfAlreadyExists);
                }
                return;
            } catch (final StorageException se) {
                final int errorCode = se.getCode();
                if (errorCode == HTTP_GONE) {
                    logger.warn(() -> format("Retrying broken resumable upload session for blob %s", blobInfo), se);
                    storageException = ExceptionsHelper.useOrSuppress(storageException, se);
                    continue;
                } else if (failIfAlreadyExists && errorCode == HTTP_PRECON_FAILED) {
                    throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
                }
                if (storageException != null) {
                    se.addSuppressed(storageException);
                }
                throw se;
            }
        }
        assert storageException != null;
        throw storageException;
    }

    /**
     * Uploads a blob using the "resumable upload" method (multiple requests, which
     * can be independently retried in case of failure, see
     * https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
     * @param purpose the operation purpose
     * @param blobInfo the info for the blob to be uploaded
     * @param inputStream the stream containing the blob data
     * @param size expected size of the blob to be written
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     */
    private void writeBlobResumable(
        OperationPurpose purpose,
        BlobInfo blobInfo,
        InputStream inputStream,
        long size,
        boolean failIfAlreadyExists
    ) throws IOException {
        // We retry 410 GONE errors to cover the unlikely but possible scenario where a resumable upload session becomes broken and
        // needs to be restarted from scratch. Given how unlikely a 410 error should be according to SLAs we retry only twice.
        assert inputStream.markSupported();
        inputStream.mark(Integer.MAX_VALUE);
        final byte[] buffer = new byte[size < bufferSize ? Math.toIntExact(size) : bufferSize];
        StorageException storageException = null;
        final Storage.BlobWriteOption[] writeOptions;
        if (blobInfo.getMd5() == null) {
            // no md5, use options without checksum validation
            writeOptions = failIfAlreadyExists ? NO_OVERWRITE_NO_MD5 : OVERWRITE_NO_MD5;
        } else {
            // md5 value is set so we use it by enabling checksum validation
            writeOptions = failIfAlreadyExists ? NO_OVERWRITE_CHECK_MD5 : OVERWRITE_CHECK_MD5;
        }
        for (int retry = 0; retry < 3; ++retry) {
            try {
                final WriteChannel writeChannel = client().meteredWriter(purpose, blobInfo, writeOptions);
                /*
                 * It is not enough to wrap the call to Streams#copy, we have to wrap the privileged calls too; this is because Streams#copy
                 * is in the stacktrace and is not granted the permissions needed to close and write the channel.
                 */
                org.elasticsearch.core.Streams.copy(inputStream, Channels.newOutputStream(new WritableBlobChannel(writeChannel)), buffer);
                writeChannel.close();
                return;
            } catch (final StorageException se) {
                final int errorCode = se.getCode();
                if (errorCode == HTTP_GONE) {
                    logger.warn(() -> format("Retrying broken resumable upload session for blob %s", blobInfo), se);
                    storageException = ExceptionsHelper.useOrSuppress(storageException, se);
                    inputStream.reset();
                    continue;
                } else if (failIfAlreadyExists && errorCode == HTTP_PRECON_FAILED) {
                    throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
                }
                if (storageException != null) {
                    se.addSuppressed(storageException);
                }
                throw se;
            }
        }
        assert storageException != null;
        throw storageException;
    }

    /**
     * Uploads a blob using the "multipart upload" method (a single
     * 'multipart/related' request containing both data and metadata. The request is
     * gziped), see:
     * https://cloud.google.com/storage/docs/json_api/v1/how-tos/multipart-upload
     * @param purpose the operation purpose
     * @param blobInfo the info for the blob to be uploaded
     * @param buffer the byte array containing the data
     * @param offset offset at which the blob contents start in the buffer
     * @param blobSize the size
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     */
    private void writeBlobMultipart(
        OperationPurpose purpose,
        BlobInfo blobInfo,
        byte[] buffer,
        int offset,
        int blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        assert blobSize <= getLargeBlobThresholdInBytes() : "large blob uploads should use the resumable upload method";
        try {
            final Storage.BlobTargetOption[] targetOptions = failIfAlreadyExists
                ? new Storage.BlobTargetOption[] { Storage.BlobTargetOption.doesNotExist() }
                : new Storage.BlobTargetOption[0];
            client().meteredCreate(purpose, blobInfo, buffer, offset, blobSize, targetOptions);
        } catch (final StorageException se) {
            if (failIfAlreadyExists && se.getCode() == HTTP_PRECON_FAILED) {
                throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
            }
            throw se;
        }
    }

    /**
     * Deletes the given path and all its children.
     *
     * @param purpose the operation purpose
     * @param pathStr Name of path to delete
     */
    DeleteResult deleteDirectory(OperationPurpose purpose, String pathStr) throws IOException {
        DeleteResult deleteResult = DeleteResult.ZERO;
        MeteredStorage.MeteredBlobPage meteredPage = client().meteredList(purpose, bucketName, BlobListOption.prefix(pathStr));
        do {
            final AtomicLong blobsDeleted = new AtomicLong(0L);
            final AtomicLong bytesDeleted = new AtomicLong(0L);
            var blobs = meteredPage.getValues().iterator();
            deleteBlobs(purpose, new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return blobs.hasNext();
                }

                @Override
                public String next() {
                    final Blob next = blobs.next();
                    blobsDeleted.incrementAndGet();
                    bytesDeleted.addAndGet(next.getSize());
                    return next.getName();
                }
            });
            deleteResult = deleteResult.add(blobsDeleted.get(), bytesDeleted.get());
            meteredPage = meteredPage.getNextPage();
        } while (meteredPage != null);
        return deleteResult;
    }

    private static String buildKey(String keyPath, String s) {
        assert s != null;
        return keyPath + s;
    }

    @Override
    public Map<String, BlobStoreActionStats> stats() {
        return statsCollector.operationsStats(storageService.isServerless());
    }

    private static final class WritableBlobChannel implements WritableByteChannel {

        private final WriteChannel channel;

        WritableBlobChannel(WriteChannel writeChannel) {
            this.channel = writeChannel;
        }

        @SuppressForbidden(reason = "channel is based on a socket")
        @Override
        public int write(final ByteBuffer src) throws IOException {
            try {
                return channel.write(src);
            } catch (IOException e) {
                // BaseStorageWriteChannel#write wraps StorageException in an IOException, but BaseStorageWriteChannel#close
                // does not, if we unwrap StorageExceptions here, it simplifies our retry-on-gone logic
                final StorageException storageException = (StorageException) ExceptionsHelper.unwrap(e, StorageException.class);
                if (storageException != null) {
                    throw storageException;
                }
                throw e;
            }
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() {
            // we manually close the channel later to have control over whether or not we want to finalize a blob
        }
    }

    OptionalBytesReference getRegister(OperationPurpose purpose, String blobName, String container, String key) throws IOException {
        final var blobId = BlobId.of(bucketName, blobName);
        try (var meteredReadChannel = client().meteredReader(purpose, blobId); var stream = Channels.newInputStream(meteredReadChannel)) {
            return OptionalBytesReference.of(BlobContainerUtils.getRegisterUsingConsistentRead(stream, container, key));
        } catch (Exception e) {
            final var serviceException = unwrapServiceException(e);
            if (serviceException != null) {
                final var statusCode = serviceException.getCode();
                if (statusCode == RestStatus.NOT_FOUND.getStatus()) {
                    return OptionalBytesReference.EMPTY;
                }
            }
            throw e;
        }
    }

    OptionalBytesReference compareAndExchangeRegister(
        OperationPurpose purpose,
        String blobName,
        String container,
        String key,
        BytesReference expected,
        BytesReference updated
    ) throws IOException {
        BlobContainerUtils.ensureValidRegisterContent(updated);

        final var blobId = BlobId.of(bucketName, blobName);
        final var blob = client().meteredGet(purpose, blobId);
        final long generation;

        if (blob == null || blob.getGeneration() == null) {
            if (expected.length() != 0) {
                return OptionalBytesReference.EMPTY;
            }
            generation = 0L;
        } else {
            generation = blob.getGeneration();
            try (
                var stream = Channels.newInputStream(
                    client().meteredReader(purpose, blobId, Storage.BlobSourceOption.generationMatch(generation))
                )
            ) {
                final var witness = BlobContainerUtils.getRegisterUsingConsistentRead(stream, container, key);
                if (witness.equals(expected) == false) {
                    return OptionalBytesReference.of(witness);
                }
            } catch (Exception e) {
                final var serviceException = unwrapServiceException(e);
                if (serviceException != null) {
                    final var statusCode = serviceException.getCode();
                    if (statusCode == RestStatus.NOT_FOUND.getStatus()) {
                        return expected.length() == 0 ? OptionalBytesReference.MISSING : OptionalBytesReference.EMPTY;
                    } else if (statusCode == RestStatus.PRECONDITION_FAILED.getStatus()) {
                        return OptionalBytesReference.MISSING;
                    }
                }
                throw e;
            }
        }

        final var blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, blobName, generation))
            .setMd5(Base64.getEncoder().encodeToString(MessageDigests.digest(updated, MessageDigests.md5())))
            .build();
        final var bytesRef = updated.toBytesRef();

        final Iterator<TimeValue> retries = casBackoffPolicy.iterator();
        BaseServiceException finalException = null;
        while (true) {
            try {
                client().meteredCreate(
                    purpose,
                    blobInfo,
                    bytesRef.bytes,
                    bytesRef.offset,
                    bytesRef.length,
                    Storage.BlobTargetOption.generationMatch()
                );
                return OptionalBytesReference.of(expected);
            } catch (Exception e) {
                final var serviceException = unwrapServiceException(e);
                if (serviceException == null) {
                    throw e;
                }
                final var statusCode = serviceException.getCode();
                if (statusCode == RestStatus.PRECONDITION_FAILED.getStatus()) {
                    return OptionalBytesReference.MISSING;
                }
                if (statusCode == TOO_MANY_REQUESTS.getStatus()) {
                    finalException = ExceptionsHelper.useOrSuppress(finalException, serviceException);
                    if (retries.hasNext()) {
                        try {
                            // noinspection BusyWait
                            Thread.sleep(retries.next().millis());
                        } catch (InterruptedException iex) {
                            Thread.currentThread().interrupt();
                            finalException.addSuppressed(iex);
                        }
                    } else {
                        throw finalException;
                    }
                }
            }
        }
    }

    // GCS retry codes https://docs.cloud.google.com/storage/docs/retry-strategy#java
    private static boolean isRetryErrCode(int code) {
        final var status = RestStatus.fromCode(code);
        if (status == null) {
            return false;
        }
        return switch (status) {
            case REQUEST_TIMEOUT, TOO_MANY_REQUESTS, INTERNAL_SERVER_ERROR, BAD_GATEWAY, SERVICE_UNAVAILABLE, GATEWAY_TIMEOUT -> true;
            default -> false;
        };
    }

    /**
     * Deletes multiple blobs from the specific bucket using a batch request
     *
     * @param blobNames names of the blobs to delete
     */
    void deleteBlobs(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
        if (blobNames.hasNext() == false) {
            return;
        }

        record DeleteResult(BlobId blobId, StorageBatchResult<Boolean> result) {}
        record DeleteFailure(BlobId blobId, int errCode) {}

        // The following algorithm maximizes the size of every batch by merging retryable items
        // from the previous batch and new items. When batch results have failed items, we first retry
        // only a single item using the SDK client's retry strategy (exponential-backoff). Retrying
        // a single item should provide enough time to back-off from throttling or temporary GCS
        // failures. Once a single item successfully retries, we proceed with the next batch, combining
        // the remaining failures and new items.
        //
        // Which is roughly looks like this:
        // - create batch of 100 new items
        // - submit batch
        // - receive 100 results, with 10 retryable failures
        // - retry 1 failure using non-batched delete using SDK retry strategy
        // - (loop) create batch from 9 remaining failures and 91 new items

        final var batchResults = new ArrayList<DeleteResult>(MAX_DELETES_PER_BATCH);
        final var batchFailures = new ArrayList<DeleteFailure>(MAX_DELETES_PER_BATCH);
        while (blobNames.hasNext() || batchFailures.isEmpty() == false) {

            // create a new batch from failed and new items, failed first
            final var batch = client().batch();
            for (var failure : batchFailures) {
                batchResults.add(new DeleteResult(failure.blobId, batch.delete(failure.blobId)));
            }
            batchFailures.clear();
            while (blobNames.hasNext() && batchResults.size() < MAX_DELETES_PER_BATCH) {
                final var blobId = BlobId.of(bucketName, blobNames.next());
                batchResults.add(new DeleteResult(blobId, batch.delete(blobId)));
            }

            // The whole batch request uses GCS client's retry logic, but individual item failures are not retried.
            // Collect all retryable failures or terminate on non-retryable error.
            try {
                batch.submit();
            } catch (Exception e) {
                throw new IOException("Failed to execute batch", e);
            }
            StorageException nonRetryableException = null;
            for (var deleteResult : batchResults) {
                try {
                    deleteResult.result.get();
                } catch (StorageException e) {
                    final var errCode = e.getCode();
                    batchFailures.add(new DeleteFailure(deleteResult.blobId, e.getCode()));
                    if (nonRetryableException == null && isRetryErrCode(errCode) == false) {
                        nonRetryableException = e;
                    }
                }
            }
            if (nonRetryableException != null) {
                throw new IOException(
                    "One or more batch items failed, non-retryable exception; all batch failures: " + batchFailures,
                    nonRetryableException
                );
            }
            batchResults.clear();

            // Delete single item using GCS client's retry logic.
            // It should provide enough back-off before trying next batch.
            if (batchFailures.isEmpty() == false) {
                final var retryBlobId = batchFailures.getLast().blobId;
                try {
                    client().delete(purpose, retryBlobId);
                    // remaining items go into the next batch
                    batchFailures.removeLast();
                } catch (StorageException e) {
                    throw new IOException(
                        "Failed to retry single batch item, blobId=" + retryBlobId + "; all batch failures: " + batchFailures,
                        e
                    );
                }
            }
        }
    }

    private static BaseServiceException unwrapServiceException(Throwable t) {
        for (int i = 0; i < 10; i++) {
            if (t == null) {
                break;
            }
            if (t instanceof BaseServiceException baseServiceException) {
                return baseServiceException;
            }
            t = t.getCause();
        }
        return null;
    }
}
