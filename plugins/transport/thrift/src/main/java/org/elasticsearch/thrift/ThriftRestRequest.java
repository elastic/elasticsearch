/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.thrift;

import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.rest.support.AbstractRestRequest;
import org.elasticsearch.rest.support.RestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author kimchy (shay.banon)
 */
public class ThriftRestRequest extends AbstractRestRequest implements org.elasticsearch.rest.RestRequest {

    private final org.elasticsearch.thrift.RestRequest request;

    private final String path;

    private final Map<String, String> params;

    public ThriftRestRequest(org.elasticsearch.thrift.RestRequest request) {
        this.request = request;
        this.params = request.getParams() == null ? new HashMap<String, String>() : request.getParams();

        int pathEndPos = request.getUri().indexOf('?');
        if (pathEndPos < 0) {
            this.path = request.getUri();
        } else {
            this.path = request.getUri().substring(0, pathEndPos);
            RestUtils.decodeQueryString(request.getUri(), pathEndPos + 1, params);
        }
    }

    @Override public Method method() {
        switch (request.getMethod()) {
            case GET:
                return Method.GET;
            case POST:
                return Method.POST;
            case PUT:
                return Method.PUT;
            case DELETE:
                return Method.DELETE;
            case HEAD:
                return Method.HEAD;
            case OPTIONS:
                return Method.OPTIONS;
        }
        return null;
    }

    @Override public String uri() {
        return request.getUri();
    }

    @Override public String path() {
        return this.path;
    }

    @Override public boolean hasContent() {
        return request.getBody() != null && request.getBody().remaining() > 0;
    }

    @Override public boolean contentUnsafe() {
        return false;
    }

    @Override public byte[] contentByteArray() {
        return request.getBody().array();
    }

    @Override public int contentByteArrayOffset() {
        return request.getBody().arrayOffset();
    }

    @Override public int contentLength() {
        return request.getBody().remaining();
    }

    @Override public String contentAsString() {
        return Unicode.fromBytes(contentByteArray(), contentByteArrayOffset(), contentLength());
    }

    @Override public Set<String> headerNames() {
        if (request.getHeaders() == null) {
            return ImmutableSet.of();
        }
        return request.getHeaders().keySet();
    }

    @Override public String header(String name) {
        if (request.getHeaders() == null) {
            return null;
        }
        return request.getHeaders().get(name);
    }

    @Override public String cookie() {
        return null;
    }

    @Override public boolean hasParam(String key) {
        return params.containsKey(key);
    }

    @Override public String param(String key) {
        return params.get(key);
    }

    @Override public Map<String, String> params() {
        return params;
    }

    @Override public String param(String key, String defaultValue) {
        String value = params.get(key);
        if (value == null) {
            return value;
        }
        return defaultValue;
    }
}
