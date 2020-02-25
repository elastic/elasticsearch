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
package org.elasticsearch.repositories.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

/**
 * Wrapper around reads from GCS that will retry blob downloads that fail part-way through, resuming from where the failure occurred.
 * This should be handled by the SDK but it isn't today. This should be revisited in the future (e.g. before removing
 * the {@link org.elasticsearch.Version#V_7_0_0} version constant) and removed if the SDK handles retries itself in the future.
 */
class GoogleCloudStorageRetryingInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageRetryingInputStream.class);

    static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

    private final Storage client;

    private final BlobId blobId;

    private final int maxRetries;

    private InputStream currentStream;
    private int attempt = 1;
    private List<StorageException> failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
    private long currentOffset;
    private boolean closed;

    GoogleCloudStorageRetryingInputStream(Storage client, BlobId blobId) throws IOException {
        this.client = client;
        this.blobId = blobId;
        this.maxRetries = client.getOptions().getRetrySettings().getMaxAttempts() + 1;
        currentStream = openStream();
    }

    private InputStream openStream() throws IOException {
        try {
            final ReadChannel readChannel = SocketAccess.doPrivilegedIOException(() -> client.reader(blobId));
            if (currentOffset > 0L) {
                readChannel.seek(currentOffset);
            }
            return Channels.newInputStream(new ReadableByteChannel() {
                @SuppressForbidden(reason = "Channel is based of a socket not a file")
                @Override
                public int read(ByteBuffer dst) throws IOException {
                    try {
                        return SocketAccess.doPrivilegedIOException(() -> readChannel.read(dst));
                    } catch (StorageException e) {
                        if (e.getCode() == HTTP_NOT_FOUND) {
                            throw new NoSuchFileException("Blob [" + blobId.getName() + "] does not exist");
                        }
                        throw e;
                    }
                }

                @Override
                public boolean isOpen() {
                    return readChannel.isOpen();
                }

                @Override
                public void close() throws IOException {
                    SocketAccess.doPrivilegedVoidIOException(readChannel::close);
                }
            });
        } catch (StorageException e) {
            throw addSuppressedExceptions(e);
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
            } catch (StorageException e) {
                reopenStreamOrFail(e);
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
            } catch (StorageException e) {
                reopenStreamOrFail(e);
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            assert false : "using GoogleCloudStorageRetryingInputStream after close";
            throw new IllegalStateException("using GoogleCloudStorageRetryingInputStream after close");
        }
    }

    private void reopenStreamOrFail(StorageException e) throws IOException {
        if (attempt >= maxRetries) {
            throw addSuppressedExceptions(e);
        }
        logger.debug(new ParameterizedMessage("failed reading [{}] at offset [{}], attempt [{}] of [{}], retrying",
            blobId, currentOffset, attempt, MAX_SUPPRESSED_EXCEPTIONS), e);
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
