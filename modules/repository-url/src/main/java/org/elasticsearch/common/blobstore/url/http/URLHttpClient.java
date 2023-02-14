/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url.http;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.rest.RestStatus;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class URLHttpClient implements Closeable {
    public static final int MAX_ERROR_MESSAGE_BODY_SIZE = 1024;
    private static final int MAX_CONNECTIONS = 50;
    private final Logger logger = LogManager.getLogger(URLHttpClient.class);

    private final CloseableHttpClient client;
    private final URLHttpClientSettings httpClientSettings;

    public static class Factory implements Closeable {
        private final PoolingHttpClientConnectionManager connManager;

        public Factory() {
            this.connManager = new PoolingHttpClientConnectionManager();
            connManager.setDefaultMaxPerRoute(MAX_CONNECTIONS);
            connManager.setMaxTotal(MAX_CONNECTIONS);
        }

        public URLHttpClient create(URLHttpClientSettings settings) {
            final CloseableHttpClient apacheHttpClient = HttpClients.custom()
                .setSSLContext(SSLContexts.createSystemDefault())
                .setConnectionManager(connManager)
                .disableAutomaticRetries()
                .setConnectionManagerShared(true)
                .build();

            return new URLHttpClient(apacheHttpClient, settings);
        }

        @Override
        public void close() {
            connManager.close();
        }
    }

    public URLHttpClient(CloseableHttpClient client, URLHttpClientSettings httpClientSettings) {
        this.client = client;
        this.httpClientSettings = httpClientSettings;
    }

    /**
     * Executes a GET Http request against the given {@code uri}, if the response it is not
     * successful (2xx status code) it throws an {@link URLHttpClientException}. If there's
     * an IO error it throws an {@link URLHttpClientIOException}.
     */
    public HttpResponse get(URI uri, Map<String, String> headers) throws IOException {
        final HttpGet request = new HttpGet(uri);
        for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
            request.setHeader(headerEntry.getKey(), headerEntry.getValue());
        }

        final RequestConfig requestConfig = RequestConfig.custom()
            .setSocketTimeout(httpClientSettings.getSocketTimeoutMs())
            .setConnectionRequestTimeout(httpClientSettings.getConnectionPoolTimeoutMs())
            .setConnectTimeout(httpClientSettings.getConnectionTimeoutMs())
            .build();
        request.setConfig(requestConfig);

        try {
            return executeRequest(request);
        } catch (IOException e) {
            throw new URLHttpClientIOException(e.getMessage(), e);
        }
    }

    private HttpResponse executeRequest(HttpGet request) throws IOException {
        final CloseableHttpResponse response = client.execute(request);

        final int statusCode = response.getStatusLine().getStatusCode();

        if (isSuccessful(statusCode) == false) {
            handleInvalidResponse(response);
        }

        return new HttpResponse() {
            @Override
            public HttpResponseInputStream getInputStream() throws IOException {
                try {
                    return new HttpResponseInputStream(request, response);
                } catch (IOException e) {
                    // Release the underlying connection in case of failure
                    IOUtils.closeWhileHandlingException(response);
                    throw e;
                }
            }

            @Override
            public String getBodyAsString(int maxSize) {
                return parseBodyAsString(response, maxSize);
            }

            @Override
            public int getStatusCode() {
                return statusCode;
            }

            @Override
            public String getHeader(String headerName) {
                final Header header = response.getFirstHeader(headerName);
                return header == null ? null : header.getValue();
            }

            @Override
            public void close() throws IOException {
                response.close();
            }
        };
    }

    private void handleInvalidResponse(CloseableHttpResponse response) {
        int statusCode = response.getStatusLine().getStatusCode();
        String errorBody = parseBodyAsString(response, MAX_ERROR_MESSAGE_BODY_SIZE);
        throw new URLHttpClientException(statusCode, createErrorMessage(statusCode, errorBody));
    }

    static String createErrorMessage(int statusCode, String errorMessage) {
        if (errorMessage.isEmpty() == false) {
            return statusCode + ": " + errorMessage;
        } else {
            return Integer.toString(statusCode);
        }
    }

    private String parseBodyAsString(CloseableHttpResponse response, int maxSize) {
        String errorMessage = "";
        InputStream bodyContent = null;
        try {
            final HttpEntity httpEntity = response.getEntity();
            if (httpEntity != null && isValidContentTypeToParseError(httpEntity)) {
                // Safeguard to avoid storing large error messages in memory
                byte[] errorBodyBytes = new byte[(int) Math.min(httpEntity.getContentLength(), maxSize)];
                bodyContent = httpEntity.getContent();
                int bytesRead = Streams.readFully(bodyContent, errorBodyBytes);
                if (bytesRead > 0) {
                    final Charset utf = getCharset(httpEntity);
                    errorMessage = new String(errorBodyBytes, utf);
                }
            }
        } catch (Exception e) {
            logger.warn("Unable to parse HTTP body to produce an error response", e);
        } finally {
            IOUtils.closeWhileHandlingException(bodyContent);
            IOUtils.closeWhileHandlingException(response);
        }
        return errorMessage;
    }

    private Charset getCharset(HttpEntity httpEntity) {
        final Header contentType = httpEntity.getContentType();
        if (contentType == null) {
            return StandardCharsets.UTF_8;
        }
        for (HeaderElement element : contentType.getElements()) {
            final NameValuePair charset = element.getParameterByName("charset");
            if (charset != null) {
                return Charset.forName(charset.getValue());
            }
        }
        // Fallback to UTF-8 and try to encode the error message with that
        return StandardCharsets.UTF_8;
    }

    private boolean isValidContentTypeToParseError(HttpEntity httpEntity) {
        Header contentType = httpEntity.getContentType();
        return contentType != null
            && httpEntity.getContentLength() > 0
            && (contentType.getValue().startsWith("text/") || contentType.getValue().startsWith("application/"));
    }

    private boolean isSuccessful(int statusCode) {
        return statusCode / 100 == RestStatus.OK.getStatus() / 100;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    interface HttpResponse extends Closeable {
        HttpResponseInputStream getInputStream() throws IOException;

        int getStatusCode();

        String getBodyAsString(int maxSize);

        @Nullable
        String getHeader(String headerName);
    }
}
