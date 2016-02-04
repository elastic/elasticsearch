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
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.cache.recycler.MockPageCacheRecycler;
import org.elasticsearch.threadpool.ThreadPool;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.*;

public class NettyHttpChannelTests extends ESTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private NettyHttpServerTransport httpServerTransport;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Settings.EMPTY);
        threadPool = new ThreadPool("test");
        MockPageCacheRecycler mockPageCacheRecycler = new MockPageCacheRecycler(Settings.EMPTY, threadPool);
        bigArrays = new MockBigArrays(mockPageCacheRecycler, new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        if (httpServerTransport != null) {
            httpServerTransport.close();
        }
    }

    @Test
    public void testCorsEnabledWithoutAllowOrigins() {
        // Set up a HTTP transport with only the CORS enabled setting
        Settings settings = Settings.builder()
                .put(NettyHttpServerTransport.SETTING_CORS_ENABLED, true)
                .build();
        httpServerTransport = new NettyHttpServerTransport(settings, networkService, bigArrays);
        HttpRequest httpRequest = new TestHttpRequest();
        httpRequest.headers().add(HttpHeaders.Names.ORIGIN, "remote");
        httpRequest.headers().add(HttpHeaders.Names.USER_AGENT, "Mozilla fake");
        WriteCapturingChannel writeCapturingChannel = new WriteCapturingChannel();
        NettyHttpRequest request = new NettyHttpRequest(httpRequest, writeCapturingChannel);

        // send a response
        NettyHttpChannel channel = new NettyHttpChannel(httpServerTransport, request, null, randomBoolean());
        channel.sendResponse(new TestReponse());

        // inspect what was written
        List<Object> writtenObjects = writeCapturingChannel.getWrittenObjects();
        assertThat(writtenObjects.size(), is(1));
        HttpResponse response = (HttpResponse) writtenObjects.get(0);
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN), nullValue());
    }

    @Test
    public void testCorsEnabledWithAllowOrigins() {
        // create a http transport with CORS enabled and allow origin configured
        Settings settings = Settings.builder()
                .put(NettyHttpServerTransport.SETTING_CORS_ENABLED, true)
                .put(NettyHttpServerTransport.SETTING_CORS_ALLOW_ORIGIN, "remote-host")
                .build();
        httpServerTransport = new NettyHttpServerTransport(settings, networkService, bigArrays);
        HttpRequest httpRequest = new TestHttpRequest();
        httpRequest.headers().add(HttpHeaders.Names.ORIGIN, "remote");
        httpRequest.headers().add(HttpHeaders.Names.USER_AGENT, "Mozilla fake");
        WriteCapturingChannel writeCapturingChannel = new WriteCapturingChannel();
        NettyHttpRequest request = new NettyHttpRequest(httpRequest, writeCapturingChannel);

        NettyHttpChannel channel = new NettyHttpChannel(httpServerTransport, request, null, randomBoolean());
        channel.sendResponse(new TestReponse());

        // inspect what was written
        List<Object> writtenObjects = writeCapturingChannel.getWrittenObjects();
        assertThat(writtenObjects.size(), is(1));
        HttpResponse response = (HttpResponse) writtenObjects.get(0);
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        String allowedOrigins = response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is("remote-host"));
    }

    private static class WriteCapturingChannel implements Channel {

        private List<Object> writtenObjects = new ArrayList<>();

        @Override
        public Integer getId() {
            return null;
        }

        @Override
        public ChannelFactory getFactory() {
            return null;
        }

        @Override
        public Channel getParent() {
            return null;
        }

        @Override
        public ChannelConfig getConfig() {
            return null;
        }

        @Override
        public ChannelPipeline getPipeline() {
            return null;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public boolean isBound() {
            return false;
        }

        @Override
        public boolean isConnected() {
            return false;
        }

        @Override
        public SocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public ChannelFuture write(Object message) {
            writtenObjects.add(message);
            return null;
        }

        @Override
        public ChannelFuture write(Object message, SocketAddress remoteAddress) {
            writtenObjects.add(message);
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture unbind() {
            return null;
        }

        @Override
        public ChannelFuture close() {
            return null;
        }

        @Override
        public ChannelFuture getCloseFuture() {
            return null;
        }

        @Override
        public int getInterestOps() {
            return 0;
        }

        @Override
        public boolean isReadable() {
            return false;
        }

        @Override
        public boolean isWritable() {
            return false;
        }

        @Override
        public ChannelFuture setInterestOps(int interestOps) {
            return null;
        }

        @Override
        public ChannelFuture setReadable(boolean readable) {
            return null;
        }

        @Override
        public boolean getUserDefinedWritability(int index) {
            return false;
        }

        @Override
        public void setUserDefinedWritability(int index, boolean isWritable) {

        }

        @Override
        public Object getAttachment() {
            return null;
        }

        @Override
        public void setAttachment(Object attachment) {

        }

        @Override
        public int compareTo(Channel o) {
            return 0;
        }

        public List<Object> getWrittenObjects() {
            return writtenObjects;
        }
    }

    private static class TestHttpRequest implements HttpRequest {

        private HttpHeaders headers = new DefaultHttpHeaders();

        @Override
        public HttpMethod getMethod() {
            return null;
        }

        @Override
        public void setMethod(HttpMethod method) {

        }

        @Override
        public String getUri() {
            return "";
        }

        @Override
        public void setUri(String uri) {

        }

        @Override
        public HttpVersion getProtocolVersion() {
            return HttpVersion.HTTP_1_1;
        }

        @Override
        public void setProtocolVersion(HttpVersion version) {

        }

        @Override
        public HttpHeaders headers() {
            return headers;
        }

        @Override
        public ChannelBuffer getContent() {
            return ChannelBuffers.EMPTY_BUFFER;
        }

        @Override
        public void setContent(ChannelBuffer content) {

        }

        @Override
        public boolean isChunked() {
            return false;
        }

        @Override
        public void setChunked(boolean chunked) {

        }
    }

    private static class TestReponse extends RestResponse {

        @Override
        public String contentType() {
            return "text";
        }

        @Override
        public BytesReference content() {
            return BytesArray.EMPTY;
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }
    }
}
