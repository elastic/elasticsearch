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

package org.elasticsearch.http.nio;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfigBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.util.Queue;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;

public class NioHttpNettyAdaptorTests extends ESTestCase {

    public void testThing() {
        NioHttpNettyAdaptor nioHttpNettyAdaptor = new NioHttpNettyAdaptor(Settings.EMPTY, mock(BiConsumer.class),
            Netty4CorsConfigBuilder.forAnyOrigin().build(), 1024);
        EmbeddedChannel adaptor = nioHttpNettyAdaptor.getAdaptor(mock(NioSocketChannel.class));

        HttpRequest defaultFullHttpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "localhost:9090/got/got");

        EmbeddedChannel ch = new EmbeddedChannel(new HttpRequestEncoder());
        ch.writeOutbound(defaultFullHttpRequest);
        Object o = ch.readOutbound();
        int i = 0;

        adaptor.writeInbound(o);

        Object o1 = adaptor.readInbound();
        int j = 0;
    }
}
