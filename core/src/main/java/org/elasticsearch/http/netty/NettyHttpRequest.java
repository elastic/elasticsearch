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

package org.elasticsearch.http.netty;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.support.RestUtils;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class NettyHttpRequest extends RestRequest {

    private final org.jboss.netty.handler.codec.http.HttpRequest request;
    private final Channel channel;
    private final Map<String, String> params;
    private final String rawPath;
    private final BytesReference content;

    public NettyHttpRequest(org.jboss.netty.handler.codec.http.HttpRequest request, Channel channel) {
        this.request = request;
        this.channel = channel;
        this.params = new HashMap<>();
        if (request.getContent().readable()) {
            this.content = new ChannelBufferBytesReference(request.getContent());
        } else {
            this.content = BytesArray.EMPTY;
        }

        String uri = request.getUri();
        int pathEndPos = uri.indexOf('?');
        if (pathEndPos < 0) {
            this.rawPath = uri;
        } else {
            this.rawPath = uri.substring(0, pathEndPos);
            RestUtils.decodeQueryString(uri, pathEndPos + 1, params);
        }
    }

    public org.jboss.netty.handler.codec.http.HttpRequest request() {
        return this.request;
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
        return content.length() > 0;
    }

    @Override
    public BytesReference content() {
        return content;
    }

    /**
     * Returns the remote address where this rest request channel is "connected to".  The
     * returned {@link SocketAddress} is supposed to be down-cast into more
     * concrete type such as {@link java.net.InetSocketAddress} to retrieve
     * the detailed information.
     */
    @Override
    public SocketAddress getRemoteAddress() {
        return channel.getRemoteAddress();
    }

    /**
     * Returns the local address where this request channel is bound to.  The returned
     * {@link SocketAddress} is supposed to be down-cast into more concrete
     * type such as {@link java.net.InetSocketAddress} to retrieve the detailed
     * information.
     */
    @Override
    public SocketAddress getLocalAddress() {
        return channel.getLocalAddress();
    }

    public Channel getChannel() {
        return channel;
    }

    @Override
    public String header(String name) {
        return request.headers().get(name);
    }

    @Override
    public Iterable<Map.Entry<String, String>> headers() {
        return request.headers().entries();
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
