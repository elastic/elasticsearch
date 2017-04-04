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

package org.elasticsearch.test.rest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FakeRestRequest extends RestRequest {

    private final BytesReference content;
    private final Method method;
    private final SocketAddress remoteAddress;

    public FakeRestRequest() {
        this(NamedXContentRegistry.EMPTY, new HashMap<>(), new HashMap<>(), null, Method.GET, "/", null);
    }

    private FakeRestRequest(NamedXContentRegistry xContentRegistry, Map<String, List<String>> headers, Map<String, String> params,
                            BytesReference content, Method method, String path, SocketAddress remoteAddress) {
        super(xContentRegistry, params, path, headers);
        this.content = content;
        this.method = method;
        this.remoteAddress = remoteAddress;
    }

    @Override
    public Method method() {
        return method;
    }

    @Override
    public String uri() {
        return rawPath();
    }

    @Override
    public boolean hasContent() {
        return content != null;
    }

    @Override
    public BytesReference content() {
        return content;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public static class Builder {
        private final NamedXContentRegistry xContentRegistry;

        private Map<String, List<String>> headers = new HashMap<>();

        private Map<String, String> params = new HashMap<>();

        private BytesReference content;

        private String path = "/";

        private Method method = Method.GET;

        private SocketAddress address = null;

        public Builder(NamedXContentRegistry xContentRegistry) {
            this.xContentRegistry = xContentRegistry;
        }

        public Builder withHeaders(Map<String, List<String>> headers) {
            this.headers = headers;
            return this;
        }

        public Builder withParams(Map<String, String> params) {
            this.params = params;
            return this;
        }

        public Builder withContent(BytesReference content, XContentType xContentType) {
            this.content = content;
            if (xContentType != null) {
                headers.put("Content-Type", Collections.singletonList(xContentType.mediaType()));
            }
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

        public Builder withRemoteAddress(SocketAddress address) {
            this.address = address;
            return this;
        }

        public FakeRestRequest build() {
            return new FakeRestRequest(xContentRegistry, headers, params, content, method, path, address);
        }

    }

}
