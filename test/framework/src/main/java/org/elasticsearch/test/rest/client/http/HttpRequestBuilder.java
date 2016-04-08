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

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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

    private final Map<String, String> params = new HashMap<>();

    private final Map<String, String> headers = new HashMap<>();

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
        return host(NetworkAddress.format(transportAddress.address().getAddress())).port(transportAddress.address().getPort());
    }

    public HttpRequestBuilder port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Sets the path to send the request to. Url encoding needs to be applied by the caller.
     * Use {@link #pathParts(String...)} instead if the path needs to be encoded, part by part.
     */
    public HttpRequestBuilder path(String path) {
        this.path = path;
        return this;
    }

    /**
     * Sets the path by providing the different parts (without slashes), which will be properly encoded.
     */
    public HttpRequestBuilder pathParts(String... path) {
        //encode rules for path and query string parameters are different. We use URI to encode the path, and URLEncoder for each query string parameter (see addParam).
        //We need to encode each path part separately though, as each one might contain slashes that need to be escaped, which needs to be done manually.
        if (path.length == 0) {
            this.path = "/";
            return this;
        }
        StringBuilder finalPath = new StringBuilder();
        for (String pathPart : path) {
            try {
                finalPath.append('/');
                // We append "/" to the path part to handle parts that start with - or other invalid characters
                URI uri = new URI(null, null, null, -1, "/" + pathPart, null, null);
                //manually escape any slash that each part may contain
                finalPath.append(uri.getRawPath().substring(1).replaceAll("/", "%2F"));
            } catch(URISyntaxException e) {
                throw new RuntimeException("unable to build uri", e);
            }
        }
        this.path = finalPath.toString();
        return this;
    }

    public HttpRequestBuilder addParam(String name, String value) {
        try {
            this.params.put(name, URLEncoder.encode(value, "utf-8"));
            return this;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public HttpRequestBuilder addHeaders(Map<String, String> headers) {
        this.headers.putAll(headers);
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
            logger.trace("adding header [{} => {}]", entry.getKey(), entry.getValue());
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
        StringBuilder uriBuilder = new StringBuilder(protocol).append("://").append(host).append(":").append(port).append(path);
        if (params.size() > 0) {
            uriBuilder.append("?").append(params.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("&")));
        }
        //using this constructor no url encoding happens, as we did everything upfront in addParam and pathPart methods
        return URI.create(uriBuilder.toString());
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
