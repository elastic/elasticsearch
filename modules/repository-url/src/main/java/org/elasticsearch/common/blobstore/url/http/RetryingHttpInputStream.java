/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore.url.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.blobstore.url.http.URLHttpClient.MAX_ERROR_MESSAGE_BODY_SIZE;
import static org.elasticsearch.core.Strings.format;

class RetryingHttpInputStream extends InputStream {
    public static final int MAX_SUPPRESSED_EXCEPTIONS = 10;
    public static final long MAX_RANGE_VAL = Long.MAX_VALUE - 1;

    private static final Logger logger = LogManager.getLogger(RetryingHttpInputStream.class);

    private final String blobName;
    private final URI blobURI;
    private final long start;
    private final long end;
    private final int maxRetries;
    private final URLHttpClient httpClient;

    private long totalBytesRead = 0;
    private long currentStreamLastOffset = 0;
    private int retryCount = 0;
    private boolean eof = false;
    private boolean closed = false;
    private HttpResponseInputStream delegate;
    private List<Exception> failures;

    RetryingHttpInputStream(String blobName, URI blobURI, URLHttpClient httpClient, int maxRetries) {
        this(blobName, blobURI, 0, MAX_RANGE_VAL, httpClient, maxRetries);
    }

    RetryingHttpInputStream(String blobName, URI blobURI, long start, long end, URLHttpClient httpClient, int maxRetries) {
        if (start < 0L) {
            throw new IllegalArgumentException("start must be non-negative");
        }

        if (end < start || end == Long.MAX_VALUE) {
            throw new IllegalArgumentException("end must be >= start and not Long.MAX_VALUE");
        }

        this.blobName = blobName;
        this.blobURI = blobURI;
        this.start = start;
        this.end = end;
        this.httpClient = httpClient;
        this.maxRetries = maxRetries;
        this.totalBytesRead = 0;
        this.retryCount = 0;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        while (true) {
            try {
                maybeOpenInputStream();
                int bytesRead = delegate.read();
                if (bytesRead == -1) {
                    eof = true;
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
        ensureOpen();
        while (true) {
            try {
                maybeOpenInputStream();
                int bytesRead = delegate.read(b, off, len);
                if (bytesRead == -1) {
                    eof = true;
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
    public long skip(long n) throws IOException {
        // This could be optimized on a failure by re-opening stream directly to the preferred location. However, it is rarely called,
        // so for now we will rely on the default implementation which just discards bytes by reading.
        return super.skip(n);
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("RetryingHttpInputStream does not support seeking");
    }

    @Override
    public void close() throws IOException {
        maybeAbort(delegate);
        try {
            if (delegate != null) {
                delegate.close();
            }
        } finally {
            closed = true;
        }
    }

    private void maybeOpenInputStream() throws IOException {
        if (delegate == null) {
            delegate = openInputStream();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Stream already closed");
        }
    }

    private void maybeThrow(IOException e) throws IOException {
        if (retryCount >= maxRetries || e instanceof NoSuchFileException) {
            logger.debug(
                () -> format(
                    "failed reading [%s] at offset [%s], retry [%s] of [%s], giving up",
                    blobURI,
                    start + totalBytesRead,
                    retryCount,
                    maxRetries
                ),
                e
            );
            throw addSuppressedFailures(e);
        }

        logger.debug(
            () -> format(
                "failed reading [%s] at offset [%s], retry [%s] of [%s], retrying",
                blobURI,
                start + totalBytesRead,
                retryCount,
                maxRetries
            ),
            e
        );

        retryCount += 1;
        accumulateFailure(e);

        maybeAbort(delegate);
        IOUtils.closeWhileHandlingException(delegate);
        delegate = null;
    }

    /**
     * Since we're using pooled http connections if we want to cancel an on-going request,
     * we should remove that connection from the connection pool since it cannot be reused.
     */
    void maybeAbort(HttpResponseInputStream inputStream) {
        if (eof || inputStream == null) {
            return;
        }

        try {
            if (start + totalBytesRead < currentStreamLastOffset) {
                inputStream.abort();
            }
        } catch (Exception e) {
            logger.warn("Failed to abort stream before closing", e);
        }
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

    private HttpResponseInputStream openInputStream() throws IOException {
        try {
            final Map<String, String> headers = Maps.newMapWithExpectedSize(1);

            if (isRangeRead()) {
                headers.put("Range", getBytesRange(Math.addExact(start, totalBytesRead), end));
            }

            try {
                final URLHttpClient.HttpResponse response = httpClient.get(blobURI, headers);
                final int statusCode = response.getStatusCode();

                if (statusCode != RestStatus.OK.getStatus() && statusCode != RestStatus.PARTIAL_CONTENT.getStatus()) {
                    String body = response.getBodyAsString(MAX_ERROR_MESSAGE_BODY_SIZE);
                    IOUtils.closeWhileHandlingException(response);
                    throw new IOException(
                        getErrorMessage("The server returned an invalid response:" + " Status code: [" + statusCode + "] - Body: " + body)
                    );
                }

                currentStreamLastOffset = Math.addExact(Math.addExact(start, totalBytesRead), getStreamLength(response));

                return response.getInputStream();
            } catch (URLHttpClientException e) {
                if (e.getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
                    throw new NoSuchFileException("blob object [" + blobName + "] not found");
                } else {
                    throw e;
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(getErrorMessage(), e);
        }
    }

    private boolean isRangeRead() {
        return start > 0 || totalBytesRead > 0 || end < MAX_RANGE_VAL;
    }

    private long getStreamLength(URLHttpClient.HttpResponse httpResponse) {
        try {
            final String contentRange = httpResponse.getHeader("Content-Range");
            if (contentRange != null) {
                final String[] contentRangeTokens = contentRange.split("[ -/]+");
                assert contentRangeTokens.length == 4 : "Unexpected Content-Range header " + Arrays.toString(contentRangeTokens);

                long lowerBound = Long.parseLong(contentRangeTokens[1]);
                long upperBound = Long.parseLong(contentRangeTokens[2]);

                assert upperBound >= lowerBound : "Incorrect Content-Range: lower bound > upper bound " + lowerBound + "-" + upperBound;
                assert lowerBound == start + totalBytesRead : "Incorrect Content-Range: lower bound != specified lower bound";
                assert upperBound == end || upperBound <= MAX_RANGE_VAL
                    : "Incorrect Content-Range: the returned upper bound is incorrect, expected ["
                        + end
                        + "] "
                        + "got ["
                        + upperBound
                        + "]";

                return upperBound - lowerBound + 1;
            }

            final String contentLength = httpResponse.getHeader("Content-Length");
            return contentLength == null ? 0 : Long.parseLong(contentLength);

        } catch (Exception e) {
            logger.debug(() -> "Unable to parse response headers while reading [" + blobURI + "]", e);
            return MAX_RANGE_VAL;
        }
    }

    private static String getBytesRange(long lowerBound, long upperInclusiveBound) {
        return "bytes=" + lowerBound + "-" + upperInclusiveBound;
    }

    private String getErrorMessage() {
        return getErrorMessage("");
    }

    private String getErrorMessage(String extraInformation) {
        String errorMessage = "Unable to read blob [" + blobName + "]";
        if (isRangeRead()) {
            errorMessage += " range[" + start + " - " + end + "]";
        }

        if (extraInformation.isBlank() == false) {
            errorMessage += " " + extraInformation;
        }

        return errorMessage;
    }
}
