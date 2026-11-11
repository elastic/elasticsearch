/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import io.netty.handler.codec.http.HttpHeaders;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RequestParams;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FakeRestRequest extends RestRequest {

    public FakeRestRequest() {
        this(
            XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
            new FakeHttpRequest(Method.GET, "", BytesArray.EMPTY, new HashMap<>()),
            RequestParams.empty(),
            new FakeHttpChannel(null)
        );
    }

    private FakeRestRequest(XContentParserConfiguration config, HttpRequest httpRequest, RequestParams params, HttpChannel httpChannel) {
        super(config, params, httpRequest.uri(), httpRequest.getHeaders(), httpRequest, httpChannel);
    }

    public static class FakeHttpRequest implements HttpRequest {

        private final Method method;
        private final String uri;
        private final Map<String, List<String>> headers;
        private HttpBody body;
        private final Exception inboundException;
        private final String scheme;

        public FakeHttpRequest(Method method, String uri, BytesReference body, Map<String, List<String>> headers) {
            this(method, uri, body == null ? HttpBody.empty() : HttpBody.fromBytesReference(body), headers, null, "http");
        }

        public FakeHttpRequest(Method method, String uri, Map<String, List<String>> headers, HttpBody body) {
            this(method, uri, body, headers, null, "http");
        }

        private FakeHttpRequest(
            Method method,
            String uri,
            HttpBody body,
            Map<String, List<String>> headers,
            Exception inboundException,
            String scheme
        ) {
            this.method = method;
            this.uri = uri;
            this.body = body;
            this.headers = new FakeHttpHeaders(headers);
            this.inboundException = inboundException;
            this.scheme = scheme;
        }

        @Override
        public Method method() {
            return method;
        }

        @Override
        public String uri() {
            return uri;
        }

        @Override
        public String getScheme() {
            return scheme;
        }

        @Override
        public HttpBody body() {
            return body;
        }

        @Override
        public void setBody(HttpBody body) {
            this.body = body;
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return headers;
        }

        @Override
        public List<String> strictCookies() {
            return Collections.emptyList();
        }

        @Override
        public HttpVersion protocolVersion() {
            return HttpVersion.HTTP_1_1;
        }

        @Override
        public HttpRequest removeHeader(String header) {
            final var filteredHeaders = new HashMap<>(headers);
            filteredHeaders.remove(header);
            return new FakeHttpRequest(method, uri, body, filteredHeaders, inboundException, scheme);
        }

        public int contentLength() {
            return switch (body) {
                case HttpBody.Full f -> f.bytes().length();
                case HttpBody.Stream s -> {
                    var len = header("Content-Length");
                    yield len == null ? 0 : Integer.parseInt(len);
                }
            };
        }

        @Override
        public boolean hasContent() {
            return contentLength() > 0;
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference unused) {
            Map<String, String> responseHeaders = new HashMap<>();
            return new HttpResponse() {
                @Override
                public void addHeader(String name, String value) {
                    responseHeaders.put(name, value);
                }

                @Override
                public boolean containsHeader(String name) {
                    return responseHeaders.containsKey(name);
                }
            };
        }

        @Override
        public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBodyPart firstBodyPart) {
            return createResponse(status, BytesArray.EMPTY);
        }

        @Override
        public void release() {}

        @Override
        public Exception getInboundException() {
            return inboundException;
        }
    }

    /**
     * HTTP headers must be case-insensitive; this is already the case in production code, see
     * {@link org.elasticsearch.http.netty4.Netty4HttpRequest#getHttpHeadersAsMap(HttpHeaders)}.
     */
    private record FakeHttpHeaders(Map<String, List<String>> original) implements Map<String, List<String>> {

        FakeHttpHeaders(Map<String, List<String>> original) {
            this.original = original.entrySet().stream().collect(Collectors.toMap(e -> lowercase(e.getKey()), Entry::getValue));
        }

        @Override
        public int size() {
            return original.size();
        }

        @Override
        public boolean isEmpty() {
            return original.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return original.containsKey(lowercase(key));
        }

        @Override
        public boolean containsValue(Object value) {
            return original.containsValue(value);
        }

        @Override
        public List<String> get(Object key) {
            return original.get(lowercase(key));
        }

        @Override
        public List<String> put(String key, List<String> value) {
            return original.put(lowercase(key), value);
        }

        @Override
        public List<String> remove(Object key) {
            return original.remove(lowercase(key));
        }

        @Override
        public void putAll(Map<? extends String, ? extends List<String>> m) {
            m.forEach((k, v) -> put(lowercase(k), v));
        }

        @Override
        public void clear() {
            original.clear();
        }

        @Override
        public Set<String> keySet() {
            return original.keySet();
        }

        @Override
        public Collection<List<String>> values() {
            return original.values();
        }

        @Override
        public Set<Entry<String, List<String>>> entrySet() {
            return original.entrySet();
        }

        private static String lowercase(Object key) {
            return ((String) key).toLowerCase(Locale.ROOT);
        }
    }

    public static class FakeHttpChannel implements HttpChannel {

        private final InetSocketAddress remoteAddress;
        private final SubscribableListener<Void> closeFuture = new SubscribableListener<>();

        public FakeHttpChannel(InetSocketAddress remoteAddress) {
            this.remoteAddress = remoteAddress;
        }

        @Override
        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
            closeFuture.addListener(listener);
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return remoteAddress;
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            closeFuture.addListener(listener);
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
            closeFuture.onResponse(null);
        }
    }

    public static class Builder {
        private final XContentParserConfiguration parserConfig;

        private Map<String, List<String>> headers = new HashMap<>();

        private RequestParams params = RequestParams.empty();

        private HttpBody content = HttpBody.empty();

        private String path = "/";

        private Method method = Method.GET;

        private InetSocketAddress address = null;

        private Exception inboundException;

        private String scheme = "http";

        public Builder(NamedXContentRegistry registry) {
            this.parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE)
                .withRegistry(registry);
        }

        public Builder withHeaders(Map<String, List<String>> headers) {
            this.headers = headers;
            return this;
        }

        public Builder withParams(Map<String, String> params) {
            if (params != null) {
                this.params = RequestParams.fromSingleValues(params);
            }
            return this;
        }

        public Builder withMultiParams(RequestParams multiParams) {
            this.params = multiParams;
            return this;
        }

        public Builder withContent(BytesReference contentBytes, XContentType xContentType) {
            this.content = HttpBody.fromBytesReference(contentBytes);
            if (xContentType != null) {
                headers.put("Content-Type", Collections.singletonList(xContentType.mediaType()));
            }
            return this;
        }

        public Builder withBody(HttpBody body) {
            this.content = body;
            return this;
        }

        public Builder withContentLength(int length) {
            headers.put("Content-Length", List.of(String.valueOf(length)));
            return this;
        }

        public Builder withPath(String path) {
            this.path = path;
            return this;
        }

        public Builder withMethod(Method method) {
            this.method = method;
            return this;
        }

        public Builder withRemoteAddress(InetSocketAddress remoteAddress) {
            this.address = remoteAddress;
            return this;
        }

        public Builder withInboundException(Exception exception) {
            this.inboundException = exception;
            return this;
        }

        public Builder withScheme(String scheme) {
            this.scheme = scheme;
            return this;
        }

        public FakeRestRequest build() {
            FakeHttpRequest fakeHttpRequest = new FakeHttpRequest(method, path, content, headers, inboundException, scheme);
            return new FakeRestRequest(parserConfig, fakeHttpRequest, params, new FakeHttpChannel(address));
        }
    }

    public static String requestToString(RestRequest restRequest) {
        return "method=" + restRequest.method() + ",path=" + restRequest.rawPath();
    }
}
