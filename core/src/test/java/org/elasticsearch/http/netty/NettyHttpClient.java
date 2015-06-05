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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;

import java.io.Closeable;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.HOST;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Tiny helper
 */
public class NettyHttpClient implements Closeable {

    private static final Function<? super HttpResponse, String> FUNCTION_RESPONSE_TO_CONTENT = new Function<HttpResponse, String>() {
        @Override
        public String apply(HttpResponse response) {
            return response.getContent().toString(Charsets.UTF_8);
        }
    };

    private static final Function<? super HttpResponse, String> FUNCTION_RESPONSE_OPAQUE_ID = new Function<HttpResponse, String>() {
        @Override
        public String apply(HttpResponse response) {
            return response.headers().get("X-Opaque-Id");
        }
    };

    public static Collection<String> returnHttpResponseBodies(Collection<HttpResponse> responses) {
        return Collections2.transform(responses, FUNCTION_RESPONSE_TO_CONTENT);
    }

    public static Collection<String> returnOpaqueIds(Collection<HttpResponse> responses) {
        return Collections2.transform(responses, FUNCTION_RESPONSE_OPAQUE_ID);
    }

    private final ClientBootstrap clientBootstrap;

    public NettyHttpClient() {
        clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory());;
    }

    public synchronized Collection<HttpResponse> sendRequests(SocketAddress remoteAddress, String... uris) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(uris.length);
        final Collection<HttpResponse> content = Collections.synchronizedList(new ArrayList<HttpResponse>(uris.length));

        clientBootstrap.setPipelineFactory(new CountDownLatchPipelineFactory(latch, content));

        ChannelFuture channelFuture = null;
        try {
            channelFuture = clientBootstrap.connect(remoteAddress);
            channelFuture.await(1000);

            for (int i = 0; i < uris.length; i++) {
                final HttpRequest httpRequest = new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, uris[i]);
                httpRequest.headers().add(HOST, "localhost");
                httpRequest.headers().add("X-Opaque-ID", String.valueOf(i));
                channelFuture.getChannel().write(httpRequest);
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
