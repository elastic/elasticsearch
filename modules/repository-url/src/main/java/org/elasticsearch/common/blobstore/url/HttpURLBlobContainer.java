/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpURLBlobContainer extends URLBlobContainer {
    private final URLHttpClient httpClient;
    private final HttpClientSettings httpClientSettings;

    public HttpURLBlobContainer(URLBlobStore blobStore,
                                BlobPath blobPath,
                                URL path,
                                URLHttpClient httpClient,
                                HttpClientSettings httpClientSettings) {
        super(blobStore, blobPath, path);
        this.httpClient = httpClient;
        this.httpClientSettings = httpClientSettings;
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) {
        return new RetryingInputStream(blobName, position, Math.addExact(position, length) - 1, httpClientSettings.getMaxRetries());
    }

    @Override
    public InputStream readBlob(String name) {
        return new RetryingInputStream(name, 0, null, httpClientSettings.getMaxRetries());
    }

    @SuppressForbidden(reason = "We call connect in doPrivileged and provide SocketPermission")
    private InputStream getInputStream(String blobName, long lowerBound, @Nullable Long upperInclusiveBound) throws IOException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<InputStream>) () -> {
                final URI blobURI = new URL(path, blobName).toURI();
                final URLHttpClient.HttpResponse response =
                    httpClient.get(blobURI, Map.of("Range", getBytesRange(lowerBound, upperInclusiveBound)), httpClientSettings);

                if (response.getStatusCode() == 404) {
                    response.close();
                    throw new NoSuchFileException("blob object [" + blobName + "] not found");
                }

                if (response.getStatusCode() > 299) {
                    response.close();
                    throw new IOException(getErrorMessage(blobName, lowerBound, upperInclusiveBound) + response.getStatusCode());
                }

                return new FilterInputStream(response.getInputStream()) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        response.close();
                    }
                };
            });
        } catch (PrivilegedActionException e) {
            final Throwable rootCause = e.getCause();
            if (rootCause instanceof NoSuchFileException) {
                throw new NoSuchFileException("blob object [" + blobName + "] not found");
            } else {
                throw new IOException(rootCause.getMessage(), rootCause);
            }
        } catch (Exception e) {
            throw new IOException(getErrorMessage(blobName, lowerBound, upperInclusiveBound), e);
        }
    }

    private String getBytesRange(long lowerBound, @Nullable Long upperInclusiveBound) {
        String upperRangeBound = upperInclusiveBound == null ? "" : Long.toString(upperInclusiveBound);
        return "bytes=" + lowerBound + "-" + upperRangeBound;
    }

    private String getErrorMessage(String blobName, long lowerBound, Long upperBound) {
        return "Unable to read blob [" + blobName + "] range [" + lowerBound + "-" + (upperBound == null ? "" : upperBound) +"]";
    }

    private class RetryingInputStream extends InputStream {
        public static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

        private final String blobName;
        private final long initialOffset;
        @Nullable
        private final Long end;
        private final int maxRetries;
        private long totalBytesRead;
        private int retryCount;
        private InputStream delegate;
        private List<Exception> failures;

        private RetryingInputStream(String blobName, long initialOffset, @Nullable Long end, int maxRetries) {
            this.blobName = blobName;
            this.initialOffset = initialOffset;
            this.end = end;
            this.maxRetries = maxRetries;
            this.totalBytesRead = 0;
            this.retryCount = 0;
        }

        @Override
        public int read() throws IOException {
            while (true) {
                try {
                    maybeOpenInputStream();
                    int bytesRead = delegate.read();
                    if (bytesRead == -1) {
                        return -1;
                    }
                    totalBytesRead += bytesRead;
                    return bytesRead;
                } catch (IOException e) {
                    maybeThrow(e);
                }
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            while (true) {
                try {
                    maybeOpenInputStream();
                    int bytesRead = delegate.read(b, off, len);
                    if (bytesRead == -1) {
                        return -1;
                    }
                    totalBytesRead += bytesRead;
                    return bytesRead;
                } catch (IOException e) {
                    maybeThrow(e);
                }
            }
        }

        private void maybeThrow(IOException e) throws IOException {
            IOUtils.closeWhileHandlingException(delegate);
            delegate = null;
            if (retryCount >= maxRetries || e instanceof NoSuchFileException) {
                throw addSuppressedFailures(e);
            }

            retryCount += 1;
            accumulateFailure(e);
        }

        private void accumulateFailure(Exception e) {
            if (failures == null) {
                failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
            }
            if (failures.size() < MAX_SUPPRESSED_EXCEPTIONS) {
                failures.add(e);
            }
        }

        private IOException addSuppressedFailures(IOException e) {
            if (failures == null) {
                return e;
            }
            for (Exception failure : failures) {
                e.addSuppressed(failure);
            }
            return e;
        }

        private void maybeOpenInputStream() throws IOException {
            if (delegate == null) {
                delegate = getInputStream(blobName, Math.addExact(initialOffset, totalBytesRead), end);
            }
        }
    }
}
