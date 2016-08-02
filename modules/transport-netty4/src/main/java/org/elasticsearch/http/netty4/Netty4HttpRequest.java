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

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpMethod;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

public class Netty4HttpRequest extends RestRequest {

    private final FullHttpRequest request;
    private final Channel channel;
    private final BytesReference content;

    Netty4HttpRequest(FullHttpRequest request, Channel channel) {
        super(request.uri());
        this.request = request;
        this.channel = channel;
        if (request.content().isReadable()) {
            this.content = Netty4Utils.toBytesReference(request.content());
        } else {
            this.content = BytesArray.EMPTY;
        }
    }

    public FullHttpRequest request() {
        return this.request;
    }

    @Override
    public Method method() {
        HttpMethod httpMethod = request.method();
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
        return request.uri();
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
        return channel.remoteAddress();
    }

    /**
     * Returns the local address where this request channel is bound to.  The returned
     * {@link SocketAddress} is supposed to be down-cast into more concrete
     * type such as {@link java.net.InetSocketAddress} to retrieve the detailed
     * information.
     */
    @Override
    public SocketAddress getLocalAddress() {
        return channel.localAddress();
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

}
