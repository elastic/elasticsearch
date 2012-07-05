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

    private final int contentLength;

    private byte[] contentAsBytes;

    public NettyHttpRequest(org.jboss.netty.handler.codec.http.HttpRequest request) {
        this.request = request;
        this.params = new HashMap<String, String>();
        this.contentLength = request.getContent().readableBytes();

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
        return contentLength > 0;
    }

    @Override
    public int contentLength() {
        return contentLength;
    }

    @Override
    public boolean contentUnsafe() {
        // if its a copy, then its not unsafe...
        if (contentAsBytes != null) {
            return false;
        }
        // HttpMessageDecoder#content is sliced but out of freshly created buffers for each read
        return false;
        //return request.getContent().hasArray();
    }

    @Override
    public byte[] contentByteArray() {
        if (contentAsBytes != null) {
            return contentAsBytes;
        }
        if (request.getContent().hasArray()) {
            return request.getContent().array();
        }
        contentAsBytes = new byte[request.getContent().readableBytes()];
        request.getContent().getBytes(request.getContent().readerIndex(), contentAsBytes);
        // clear the content, so it can be GC'ed, we make sure to work from contentAsBytes from here on
        request.setContent(null);
        return contentAsBytes;
    }

    @Override
    public int contentByteArrayOffset() {
        if (contentAsBytes != null) {
            return 0;
        }
        if (request.getContent().hasArray()) {
            // get the array offset, and the reader index offset within it
            return request.getContent().arrayOffset() + request.getContent().readerIndex();
        }
        return 0;
    }

    @Override
    public String contentAsString() {
        if (contentAsBytes != null) {
            return new String(contentAsBytes, Charsets.UTF_8);
        }
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
