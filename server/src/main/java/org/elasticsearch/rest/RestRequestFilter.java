/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Identifies an object that supplies a filter for the content of a {@link RestRequest}. This interface should be implemented by a
 * {@link org.elasticsearch.rest.RestHandler} that expects there will be sensitive content in the body of the request such as a password
 */
public interface RestRequestFilter {

    /**
     * The list of fields that should be filtered. This can be a dot separated pattern to match sub objects and also supports wildcards
     */
    Set<String> getFilteredFields();

    default HttpRequest formatRequestContentForAuditing(HttpRequest httpRequest, XContentType xContentType) {
        return formatRequestContentForAuditing(httpRequest, xContentType, getFilteredFields());
    }

    static HttpRequest formatRequestContentForAuditing(HttpRequest httpRequest, XContentType xContentType, Set<String> fields) {
        if (httpRequest.content().length() == 0) {
            return httpRequest;
        }
        return HttpRequestWrapper.jsonFormattedHttpRequest(
            HttpRequestWrapper.filteredHttpRequest(httpRequest, xContentType, fields),
            xContentType
        );
    }

    class HttpRequestWrapper implements HttpRequest {

        protected final HttpRequest wrappedRequest;

        private HttpRequestWrapper(HttpRequest httpRequest) {
            this.wrappedRequest = httpRequest;
        }

        /**
         * Wraps the RestRequest and returns a version that provides the filtered content
         */
        static HttpRequest filteredHttpRequest(
            HttpRequest originalRequest,
            XContentType originalXContentType,
            Set<String> fieldsToFilterOut
        ) {
            if (fieldsToFilterOut.isEmpty()) {
                return originalRequest;
            } else {
                return new HttpRequestWrapper(originalRequest) {
                    private volatile BytesReference filteredContent;

                    @Override
                    public BytesReference content() {
                        if (filteredContent == null) {
                            synchronized (this) {
                                if (filteredContent == null) {
                                    filteredContent = filterContent(wrappedRequest.content(), originalXContentType, fieldsToFilterOut);
                                }
                            }
                        }
                        return filteredContent;
                    }

                    @Override
                    public HttpRequest releaseAndCopy() {
                        return filteredHttpRequest(this.wrappedRequest.releaseAndCopy(), originalXContentType, fieldsToFilterOut);
                    }

                    private static BytesReference filterContent(BytesReference toFilter, XContentType xContentType, Set<String> fields) {
                        if (toFilter.length() > 0 && fields.isEmpty() == false) {
                            Objects.requireNonNull(xContentType, "unknown content type");
                            Tuple<XContentType, Map<String, Object>> result = XContentHelper.convertToMap(toFilter, true, xContentType);
                            Map<String, Object> transformedSource = XContentMapValues.filter(
                                result.v2(),
                                null,
                                fields.toArray(Strings.EMPTY_ARRAY)
                            );
                            try {
                                XContentBuilder xContentBuilder = XContentBuilder.builder(result.v1().xContent()).map(transformedSource);
                                return BytesReference.bytes(xContentBuilder);
                            } catch (IOException e) {
                                throw new ElasticsearchException("failed to parse request", e);
                            }
                        } else {
                            return toFilter;
                        }
                    }
                };
            }
        }

        static HttpRequest jsonFormattedHttpRequest(HttpRequest originalRequest, XContentType originalXContentType) {
            return new HttpRequestWrapper(originalRequest) {
                private volatile BytesReference formattedContent;

                @Override
                public BytesReference content() {
                    if (formattedContent == null) {
                        synchronized (this) {
                            if (formattedContent == null) {
                                formattedContent = formatContent(wrappedRequest.content(), originalXContentType);
                            }
                        }
                    }
                    return formattedContent;
                }

                @Override
                public HttpRequest releaseAndCopy() {
                    return jsonFormattedHttpRequest(this.wrappedRequest.releaseAndCopy(), originalXContentType);
                }

                private static BytesReference formatContent(BytesReference toFormat, XContentType originalXContentType) {
                    if (toFormat.length() > 0) {
                        Objects.requireNonNull(originalXContentType);
                        try {
                            return XContentHelper.convertToJsonBytesReference(toFormat, false, false, originalXContentType);
                        } catch (IOException ioe) {
                            return CompositeBytesReference.of(new BytesArray("Invalid Format: "), toFormat);
                        }
                    } else {
                        return toFormat;
                    }
                }
            };
        }

        @Override
        public Method method() {
            return this.wrappedRequest.method();
        }

        @Override
        public String uri() {
            return this.wrappedRequest.uri();
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return this.wrappedRequest.getHeaders();
        }

        @Override
        public String header(String name) {
            return this.wrappedRequest.header(name);
        }

        @Override
        public BytesReference content() {
            return this.wrappedRequest.content();
        }

        @Override
        public List<String> strictCookies() {
            return this.wrappedRequest.strictCookies();
        }

        @Override
        public HttpVersion protocolVersion() {
            return this.wrappedRequest.protocolVersion();
        }

        @Override
        public HttpRequest removeHeader(String header) {
            return this.wrappedRequest.removeHeader(header);
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference content) {
            return this.wrappedRequest.createResponse(status, content);
        }

        @Override
        public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBody content) {
            return this.wrappedRequest.createResponse(status, content);
        }

        @Override
        public Exception getInboundException() {
            return this.wrappedRequest.getInboundException();
        }

        @Override
        public void release() {
            this.wrappedRequest.release();
        }

        @Override
        public HttpRequest releaseAndCopy() {
            return new HttpRequestWrapper(this.wrappedRequest.releaseAndCopy());
        }
    }
}
