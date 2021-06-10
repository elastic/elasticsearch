/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.HttpResponse;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.cloud.BaseService;
import com.google.cloud.RetryHelper;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.spi.v1.HttpStorageRpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.NoSuchFileException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Wrapper around reads from GCS that will retry blob downloads that fail part-way through, resuming from where the failure occurred.
 * This should be handled by the SDK but it isn't today. This should be revisited in the future (e.g. before removing
 * the {@link org.elasticsearch.Version#V_7_0_0} version constant) and removed if the SDK handles retries itself in the future.
 */
class GoogleCloudStorageRetryingInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageRetryingInputStream.class);

    static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

    private final Storage client;
    private final com.google.api.services.storage.Storage storage;

    private final BlobId blobId;

    private final long start;
    private final long end;

    private final int maxAttempts;

    private InputStream currentStream;
    private int attempt = 1;
    private List<StorageException> failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
    private long currentOffset;
    private boolean closed;

    GoogleCloudStorageRetryingInputStream(Storage client, BlobId blobId) throws IOException {
        this(client, blobId, 0, Long.MAX_VALUE - 1);
    }

    // both start and end are inclusive bounds, following the definition in https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    GoogleCloudStorageRetryingInputStream(Storage client, BlobId blobId, long start, long end) throws IOException {
        if (start < 0L) {
            throw new IllegalArgumentException("start must be non-negative");
        }
        if (end < start || end == Long.MAX_VALUE) {
            throw new IllegalArgumentException("end must be >= start and not Long.MAX_VALUE");
        }
        this.client = client;
        this.blobId = blobId;
        this.start = start;
        this.end = end;
        this.maxAttempts = client.getOptions().getRetrySettings().getMaxAttempts();
        SpecialPermission.check();
        storage = getStorage(client);
        currentStream = openStream();
    }

    @SuppressForbidden(reason = "need access to storage client")
    private static com.google.api.services.storage.Storage getStorage(Storage client) {
        return AccessController.doPrivileged((PrivilegedAction<com.google.api.services.storage.Storage>) () -> {
            assert client.getOptions().getRpc() instanceof HttpStorageRpc;
            assert Stream.of(client.getOptions().getRpc().getClass().getDeclaredFields()).anyMatch(f -> f.getName().equals("storage"));
            try {
                final Field storageField = client.getOptions().getRpc().getClass().getDeclaredField("storage");
                storageField.setAccessible(true);
                return (com.google.api.services.storage.Storage) storageField.get(client.getOptions().getRpc());
            } catch (Exception e) {
                throw new IllegalStateException("storage could not be set up", e);
            }
        });
    }

    private InputStream openStream() throws IOException {
        try {
            try {
                return RetryHelper.runWithRetries(() -> {
                        try {
                            return SocketAccess.doPrivilegedIOException(() -> {
                                final Get get = storage.objects().get(blobId.getBucket(), blobId.getName());
                                get.setReturnRawInputStream(true);

                                if (currentOffset > 0 || start > 0 || end < Long.MAX_VALUE - 1) {
                                    get.getRequestHeaders().setRange("bytes=" + Math.addExact(start, currentOffset) + "-" + end);
                                }
                                final HttpResponse resp = get.executeMedia();
                                final Long contentLength = resp.getHeaders().getContentLength();
                                InputStream content = resp.getContent();
                                if (contentLength != null) {
                                    content = new ContentLengthValidatingInputStream(content, contentLength);
                                }
                                return content;
                            });
                        } catch (IOException e) {
                            throw StorageException.translate(e);
                        }
                    }, client.getOptions().getRetrySettings(), BaseService.EXCEPTION_HANDLER, client.getOptions().getClock());
            } catch (RetryHelper.RetryHelperException e) {
                throw StorageException.translateAndThrow(e);
            }
        } catch (StorageException e) {
            if (e.getCode() == 404) {
                throw addSuppressedExceptions(
                    new NoSuchFileException("Blob object [" + blobId.getName() + "] not found: " + e.getMessage()));
            }
            throw addSuppressedExceptions(e);
        }
    }

    // Google's SDK ignores the Content-Length header when no bytes are sent, see NetHttpResponse.SizeValidatingInputStream
    // We have to implement our own validation logic here
    static final class ContentLengthValidatingInputStream extends FilterInputStream {
        private final long contentLength;

        private long read = 0L;

        ContentLengthValidatingInputStream(InputStream in, long contentLength) {
            super(in);
            this.contentLength = contentLength;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            final int n = in.read(b, off, len);
            if (n == -1) {
                checkContentLengthOnEOF();
            } else {
                read += n;
            }
            return n;
        }

        @Override
        public int read() throws IOException {
            final int n = in.read();
            if (n == -1) {
                checkContentLengthOnEOF();
            } else {
                read++;
            }
            return n;
        }

        @Override
        public long skip(long len) throws IOException {
            final long n = in.skip(len);
            read += n;
            return n;
        }

        private void checkContentLengthOnEOF() throws IOException {
            if (read < contentLength) {
                throw new IOException("Connection closed prematurely: read = " + read + ", Content-Length = " + contentLength);
            }
        }
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        while (true) {
            try {
                final int result = currentStream.read();
                currentOffset += 1;
                return result;
            } catch (IOException e) {
                reopenStreamOrFail(StorageException.translate(e));
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        while (true) {
            try {
                final int bytesRead = currentStream.read(b, off, len);
                if (bytesRead == -1) {
                    return -1;
                }
                currentOffset += bytesRead;
                return bytesRead;
            } catch (IOException e) {
                reopenStreamOrFail(StorageException.translate(e));
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            assert false : "using GoogleCloudStorageRetryingInputStream after close";
            throw new IllegalStateException("using GoogleCloudStorageRetryingInputStream after close");
        }
    }

    // TODO: check that object did not change when stream is reopened (e.g. based on etag)
    private void reopenStreamOrFail(StorageException e) throws IOException {
        if (attempt >= maxAttempts) {
            throw addSuppressedExceptions(e);
        }
        logger.debug(new ParameterizedMessage("failed reading [{}] at offset [{}], attempt [{}] of [{}], retrying",
            blobId, currentOffset, attempt, maxAttempts), e);
        attempt += 1;
        if (failures.size() < MAX_SUPPRESSED_EXCEPTIONS) {
            failures.add(e);
        }
        IOUtils.closeWhileHandlingException(currentStream);
        currentStream = openStream();
    }

    @Override
    public void close() throws IOException {
        currentStream.close();
        closed = true;
    }

    @Override
    public long skip(long n) {
        throw new UnsupportedOperationException("GoogleCloudStorageRetryingInputStream does not support seeking");
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("GoogleCloudStorageRetryingInputStream does not support seeking");
    }

    private <T extends Exception> T addSuppressedExceptions(T e) {
        for (StorageException failure : failures) {
            e.addSuppressed(failure);
        }
        return e;
    }
}
