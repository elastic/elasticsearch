/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.http.netty;

import com.google.common.base.Charsets;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.rest.support.AbstractRestRequest;
import org.elasticsearch.rest.support.RestUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class NettyHttpRequest extends AbstractRestRequest implements HttpRequest {

    private final org.jboss.netty.handler.codec.http.HttpRequest request;

    private final Map<String, String> params;

    private final String rawPath;

    private byte[] cachedData;

    public NettyHttpRequest(org.jboss.netty.handler.codec.http.HttpRequest request) {
        this.request = request;
        this.params = new HashMap<String, String>();

        String uri = request.getUri();
        int pathEndPos = uri.indexOf('?');
        if (pathEndPos < 0) {
            this.rawPath = uri;
        } else {
            this.rawPath = uri.substring(0, pathEndPos);
            RestUtils.decodeQueryString(uri, pathEndPos + 1, params);
        }
    }

    @Override
    public Method method() {
        HttpMethod httpMethod = request.getMethod();
        if (httpMethod == HttpMethod.GET)
            return Method.GET;

        if (httpMethod == HttpMethod.POST)
            return Method.POST;

        if (httpMethod == HttpMethod.PUT)
            return Method.PUT;

        if (httpMethod == HttpMethod.DELETE)
            return Method.DELETE;

        if (httpMethod == HttpMethod.HEAD) {
            return Method.HEAD;
        }

        if (httpMethod == HttpMethod.OPTIONS) {
            return Method.OPTIONS;
        }

        return Method.GET;
    }

    @Override
    public String uri() {
        return request.getUri();
    }

    @Override
    public String rawPath() {
        return rawPath;
    }

    @Override
    public Map<String, String> params() {
        return params;
    }

    @Override
    public boolean hasContent() {
        return request.getContent().readableBytes() > 0;
    }

    @Override
    public int contentLength() {
        return request.getContent().readableBytes();
    }

    @Override
    public boolean contentUnsafe() {
        // HttpMessageDecoder#content variable gets freshly created for each request and not reused across
        // requests
        return false;
        //return request.getContent().hasArray();
    }

    @Override
    public byte[] contentByteArray() {
        if (request.getContent().hasArray()) {
            return request.getContent().array();
        }
        if (cachedData != null) {
            return cachedData;
        }
        cachedData = new byte[request.getContent().readableBytes()];
        request.getContent().getBytes(request.getContent().readerIndex(), cachedData);
        return cachedData;
    }

    @Override
    public int contentByteArrayOffset() {
        if (request.getContent().hasArray()) {
            // get the array offset, and the reader index offset within it
            return request.getContent().arrayOffset() + request.getContent().readerIndex();
        }
        return 0;
    }

    @Override
    public String contentAsString() {
        return request.getContent().toString(Charsets.UTF_8);
    }

    @Override
    public String header(String name) {
        return request.getHeader(name);
    }

    @Override
    public boolean hasParam(String key) {
        return params.containsKey(key);
    }

    @Override
    public String param(String key) {
        return params.get(key);
    }

    @Override
    public String param(String key, String defaultValue) {
        String value = params.get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }
}
