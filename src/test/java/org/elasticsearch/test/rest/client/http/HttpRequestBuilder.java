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
package org.elasticsearch.test.rest.client.http;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Executable builder for an http request
 * Holds an {@link org.apache.http.client.HttpClient} that is used to send the built http request
 */
public class HttpRequestBuilder {

    private static final ESLogger logger = Loggers.getLogger(HttpRequestBuilder.class);

    static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");

    private final CloseableHttpClient httpClient;

    private String protocol = "http";

    private String host;

    private int port;

    private String path = "";

    private final Map<String, String> params = Maps.newHashMap();

    private final Map<String, String> headers = Maps.newHashMap();

    private String method = HttpGetWithEntity.METHOD_NAME;

    private String body;

    public HttpRequestBuilder(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public HttpRequestBuilder host(String host) {
        this.host = host;
        return this;
    }

    public HttpRequestBuilder httpTransport(HttpServerTransport httpServerTransport) {
        InetSocketTransportAddress transportAddress = (InetSocketTransportAddress) httpServerTransport.boundAddress().publishAddress();
        return host(transportAddress.address().getHostName()).port(transportAddress.address().getPort());
    }

    public HttpRequestBuilder port(int port) {
        this.port = port;
        return this;
    }

    public HttpRequestBuilder path(String path) {
        this.path = path;
        return this;
    }

    public HttpRequestBuilder addParam(String name, String value) {
        try {
            //manually url encode params, since URI does it only partially (e.g. '+' stays as is)
            this.params.put(name, URLEncoder.encode(value, "utf-8"));
            return this;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public HttpRequestBuilder addHeaders(Headers headers) {
        for (String header : headers.headers().names()) {
            this.headers.put(header, headers.headers().get(header));
        }
        return this;
    }

    public HttpRequestBuilder addHeader(String name, String value) {
        this.headers.put(name, value);
        return this;
    }

    public HttpRequestBuilder protocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    public HttpRequestBuilder method(String method) {
        this.method = method;
        return this;
    }

    public HttpRequestBuilder body(String body) {
        if (Strings.hasLength(body)) {
            this.body = body;
        }
        return this;
    }

    public HttpResponse execute() throws IOException {
        HttpUriRequest httpUriRequest = buildRequest();
        if (logger.isTraceEnabled()) {
            StringBuilder stringBuilder = new StringBuilder(httpUriRequest.getMethod()).append(" ").append(httpUriRequest.getURI());
            if (Strings.hasLength(body)) {
                stringBuilder.append("\n").append(body);
            }
            logger.trace("sending request \n{}", stringBuilder.toString());
        }
        for (Map.Entry<String, String> entry : this.headers.entrySet()) {
            httpUriRequest.addHeader(entry.getKey(), entry.getValue());
        }
        try (CloseableHttpResponse closeableHttpResponse = httpClient.execute(httpUriRequest)) {
            HttpResponse httpResponse = new HttpResponse(httpUriRequest, closeableHttpResponse);
            logger.trace("got response \n{}\n{}", closeableHttpResponse, httpResponse.hasBody() ? httpResponse.getBody() : "");
            return httpResponse;
        }
    }

    private HttpUriRequest buildRequest() {

        if (HttpGetWithEntity.METHOD_NAME.equalsIgnoreCase(method)) {
            return addOptionalBody(new HttpGetWithEntity(buildUri()));
        }

        if (HttpHead.METHOD_NAME.equalsIgnoreCase(method)) {
            checkBodyNotSupported();
            return new HttpHead(buildUri());
        }

        if (HttpOptions.METHOD_NAME.equalsIgnoreCase(method)) {
            checkBodyNotSupported();
            return new HttpOptions(buildUri());
        }

        if (HttpDeleteWithEntity.METHOD_NAME.equalsIgnoreCase(method)) {
            return addOptionalBody(new HttpDeleteWithEntity(buildUri()));
        }

        if (HttpPut.METHOD_NAME.equalsIgnoreCase(method)) {
            return addOptionalBody(new HttpPut(buildUri()));
        }

        if (HttpPost.METHOD_NAME.equalsIgnoreCase(method)) {
            return addOptionalBody(new HttpPost(buildUri()));
        }

        throw new UnsupportedOperationException("method [" + method + "] not supported");
    }

    private URI buildUri() {
        try {
            //url encode rules for path and query params are different. We use URI to encode the path, but we manually encode each query param through URLEncoder.
            URI uri = new URI(protocol, null, host, port, path, null, null);
            //String concatenation FTW. If we use the nicer multi argument URI constructor query parameters will get only partially encoded
            //(e.g. '+' will stay as is) hence when trying to properly encode params manually they will end up double encoded (+ becomes %252B instead of %2B).
            StringBuilder uriBuilder = new StringBuilder(protocol).append("://").append(host).append(":").append(port).append(uri.getRawPath());
            if (params.size() > 0) {
                uriBuilder.append("?").append(Joiner.on('&').withKeyValueSeparator("=").join(params));
            }
            return URI.create(uriBuilder.toString());
        } catch(URISyntaxException e) {
            throw new IllegalArgumentException("unable to build uri", e);
        }
    }

    private HttpEntityEnclosingRequestBase addOptionalBody(HttpEntityEnclosingRequestBase requestBase) {
        if (Strings.hasText(body)) {
            requestBase.setEntity(new StringEntity(body, DEFAULT_CHARSET));
        }
        return requestBase;
    }

    private void checkBodyNotSupported() {
        if (Strings.hasText(body)) {
            throw new IllegalArgumentException("request body not supported with head request");
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder(method).append(" '")
                .append(host).append(":").append(port).append(path).append("'");
        if (!params.isEmpty()) {
            stringBuilder.append(", params=").append(params);
        }
        if (Strings.hasLength(body)) {
            stringBuilder.append(", body=\n").append(body);
        }
        return stringBuilder.toString();
    }
}
