/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

class TestHttpRequest implements HttpRequest {

    private final Supplier<HttpVersion> version;
    private final RestRequest.Method method;
    private final String uri;
    private final HashMap<String, List<String>> headers = new HashMap<>();

    TestHttpRequest(Supplier<HttpVersion> versionSupplier, RestRequest.Method method, String uri) {
        this.version = versionSupplier;
        this.method = method;
        this.uri = uri;
    }

    TestHttpRequest(HttpVersion version, RestRequest.Method method, String uri) {
        this(() -> version, method, uri);
    }

    @Override
    public RestRequest.Method method() {
        return method;
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public BytesReference content() {
        return BytesArray.EMPTY;
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    @Override
    public List<String> strictCookies() {
        return Arrays.asList("cookie", "cookie2");
    }

    @Override
    public HttpVersion protocolVersion() {
        return version.get();
    }

    @Override
    public HttpRequest removeHeader(String header) {
        throw new UnsupportedOperationException("Do not support removing header on test request.");
    }

    @Override
    public HttpResponse createResponse(RestStatus status, BytesReference content) {
        return new TestHttpResponse(status, content);
    }

    @Override
    public void release() {
    }

    @Override
    public HttpRequest releaseAndCopy() {
        return this;
    }

    @Override
    public Exception getInboundException() {
        return null;
    }
}
