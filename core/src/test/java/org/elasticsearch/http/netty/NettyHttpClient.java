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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.HOST;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Tiny helper to send http requests over netty.
 */
public class NettyHttpClient implements Closeable {

    public static Collection<String> returnHttpResponseBodies(Collection<HttpResponse> responses) {
        List<String> list = new ArrayList<>(responses.size());
        for (HttpResponse response : responses) {
            list.add(response.getContent().toString(StandardCharsets.UTF_8));
        }
        return list;
    }

    public static Collection<String> returnOpaqueIds(Collection<HttpResponse> responses) {
        List<String> list = new ArrayList<>(responses.size());
        for (HttpResponse response : responses) {
            list.add(response.headers().get("X-Opaque-Id"));
        }
        return list;
    }

    private final ClientBootstrap clientBootstrap;

    public NettyHttpClient() {
        clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory());;
    }

    public Collection<HttpResponse> get(SocketAddress remoteAddress, String... uris) throws InterruptedException {
        Collection<HttpRequest> requests = new ArrayList<>(uris.length);
        for (int i = 0; i < uris.length; i++) {
            final HttpRequest httpRequest = new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, uris[i]);
            httpRequest.headers().add(HOST, "localhost");
            httpRequest.headers().add("X-Opaque-ID", String.valueOf(i));
            requests.add(httpRequest);
        }
        return sendRequests(remoteAddress, requests);
    }

    @SafeVarargs // Safe not because it doesn't do anything with the type parameters but because it won't leak them into other methods.
    public final Collection<HttpResponse> post(SocketAddress remoteAddress, Tuple<String, CharSequence>... urisAndBodies)
            throws InterruptedException {
        return processRequestsWithBody(HttpMethod.POST, remoteAddress, urisAndBodies);
    }

    @SafeVarargs // Safe not because it doesn't do anything with the type parameters but because it won't leak them into other methods.
    public final Collection<HttpResponse> put(SocketAddress remoteAddress, Tuple<String, CharSequence>... urisAndBodies)
            throws InterruptedException {
        return processRequestsWithBody(HttpMethod.PUT, remoteAddress, urisAndBodies);
    }

    @SafeVarargs // Safe not because it doesn't do anything with the type parameters but because it won't leak them into other methods.
    private final Collection<HttpResponse> processRequestsWithBody(HttpMethod method, SocketAddress remoteAddress, Tuple<String,
        CharSequence>... urisAndBodies) throws InterruptedException {
        Collection<HttpRequest> requests = new ArrayList<>(urisAndBodies.length);
        for (Tuple<String, CharSequence> uriAndBody : urisAndBodies) {
            ChannelBuffer content = ChannelBuffers.copiedBuffer(uriAndBody.v2(), StandardCharsets.UTF_8);
            HttpRequest request = new DefaultHttpRequest(HTTP_1_1, method, uriAndBody.v1());
            request.headers().add(HOST, "localhost");
            request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());
            request.setContent(content);
            requests.add(request);
        }
        return sendRequests(remoteAddress, requests);
    }

    private synchronized Collection<HttpResponse> sendRequests(SocketAddress remoteAddress, Collection<HttpRequest> requests)
        throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(requests.size());
        final Collection<HttpResponse> content = Collections.synchronizedList(new ArrayList<>(requests.size()));

        clientBootstrap.setPipelineFactory(new CountDownLatchPipelineFactory(latch, content));

        ChannelFuture channelFuture = null;
        try {
            channelFuture = clientBootstrap.connect(remoteAddress);
            channelFuture.await(1000);

            for (HttpRequest request : requests) {
                channelFuture.getChannel().write(request);
            }
            latch.await();

        } finally {
            if (channelFuture != null) {
                channelFuture.getChannel().close();
            }
        }

        return content;
    }

    @Override
    public void close() {
        clientBootstrap.shutdown();
        clientBootstrap.releaseExternalResources();
    }

    /**
     * helper factory which adds returned data to a list and uses a count down latch to decide when done
     */
    public static class CountDownLatchPipelineFactory implements ChannelPipelineFactory {
        private final CountDownLatch latch;
        private final Collection<HttpResponse> content;

        public CountDownLatchPipelineFactory(CountDownLatch latch, Collection<HttpResponse> content) {
            this.latch = latch;
            this.content = content;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            final int maxBytes = new ByteSizeValue(100, ByteSizeUnit.MB).bytesAsInt();
            return Channels.pipeline(
                    new HttpClientCodec(),
                    new HttpChunkAggregator(maxBytes),
                    new SimpleChannelUpstreamHandler() {
                        @Override
                        public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
                            final Object message = e.getMessage();

                            if (message instanceof HttpResponse) {
                                HttpResponse response = (HttpResponse) message;
                                content.add(response);
                            }

                            latch.countDown();
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
                            super.exceptionCaught(ctx, e);
                            latch.countDown();
                        }
                    });
        }
    }

}
