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
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.http.netty.cors.CorsHandler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.handler.codec.http.DefaultHttpHeaders;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.After;
import org.junit.Before;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NettyHttpChannelTests extends ESTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private NettyHttpServerTransport httpServerTransport;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Settings.EMPTY);
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService());
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

    public void testCorsEnabledWithoutAllowOrigins() {
        // Set up a HTTP transport with only the CORS enabled setting
        Settings settings = Settings.builder()
                .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
                .build();
        HttpResponse response = execRequestWithCors(settings, "remote-host", "request-host");
        // inspect response and validate
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN), nullValue());
    }

    public void testCorsEnabledWithAllowOrigins() {
        final String originValue = "remote-host";
        // create a http transport with CORS enabled and allow origin configured
        Settings settings = Settings.builder()
                .put(SETTING_CORS_ENABLED.getKey(), true)
                .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
                .build();
        HttpResponse response = execRequestWithCors(settings, originValue, "request-host");
        // inspect response and validate
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        String allowedOrigins = response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));
    }

    public void testCorsAllowOriginWithSameHost() {
        String originValue = "remote-host";
        String host = "remote-host";
        // create a http transport with CORS enabled
        Settings settings = Settings.builder()
                                .put(SETTING_CORS_ENABLED.getKey(), true)
                                .build();
        HttpResponse response = execRequestWithCors(settings, originValue, host);
        // inspect response and validate
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        String allowedOrigins = response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));

        originValue = "http://" + originValue;
        response = execRequestWithCors(settings, originValue, host);
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        allowedOrigins = response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));

        originValue = originValue + ":5555";
        host = host + ":5555";
        response = execRequestWithCors(settings, originValue, host);
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        allowedOrigins = response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));

        originValue = originValue.replace("http", "https");
        response = execRequestWithCors(settings, originValue, host);
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        allowedOrigins = response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));
    }

    public void testThatStringLiteralWorksOnMatch() {
        final String originValue = "remote-host";
        Settings settings = Settings.builder()
                                .put(SETTING_CORS_ENABLED.getKey(), true)
                                .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
                                .put(SETTING_CORS_ALLOW_METHODS.getKey(), "get, options, post")
                                .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
                                .build();
        HttpResponse response = execRequestWithCors(settings, originValue, "request-host");
        // inspect response and validate
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        String allowedOrigins = response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_CREDENTIALS), equalTo("true"));
    }

    public void testThatAnyOriginWorks() {
        final String originValue = CorsHandler.ANY_ORIGIN;
        Settings settings = Settings.builder()
                                .put(SETTING_CORS_ENABLED.getKey(), true)
                                .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
                                .build();
        HttpResponse response = execRequestWithCors(settings, originValue, "request-host");
        // inspect response and validate
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        String allowedOrigins = response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));
        assertThat(response.headers().get(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_CREDENTIALS), nullValue());
    }

    public void testHeadersSet() {
        Settings settings = Settings.builder().build();
        httpServerTransport = new NettyHttpServerTransport(settings, networkService, bigArrays, threadPool);
        HttpRequest httpRequest = new TestHttpRequest();
        httpRequest.headers().add(HttpHeaders.Names.ORIGIN, "remote");
        WriteCapturingChannel writeCapturingChannel = new WriteCapturingChannel();
        NettyHttpRequest request = new NettyHttpRequest(httpRequest, writeCapturingChannel);

        // send a response
        NettyHttpChannel channel = new NettyHttpChannel(httpServerTransport, request, null, randomBoolean());
        TestReponse resp = new TestReponse();
        final String customHeader = "custom-header";
        final String customHeaderValue = "xyz";
        resp.addHeader(customHeader, customHeaderValue);
        channel.sendResponse(resp);

        // inspect what was written
        List<Object> writtenObjects = writeCapturingChannel.getWrittenObjects();
        assertThat(writtenObjects.size(), is(1));
        HttpResponse response = (HttpResponse) writtenObjects.get(0);
        assertThat(response.headers().get("non-existent-header"), nullValue());
        assertThat(response.headers().get(customHeader), equalTo(customHeaderValue));
        assertThat(response.headers().get(HttpHeaders.Names.CONTENT_LENGTH), equalTo(Integer.toString(resp.content().length())));
        assertThat(response.headers().get(HttpHeaders.Names.CONTENT_TYPE), equalTo(resp.contentType()));
    }

    private HttpResponse execRequestWithCors(final Settings settings, final String originValue, final String host) {
        // construct request and send it over the transport layer
        httpServerTransport = new NettyHttpServerTransport(settings, networkService, bigArrays, threadPool);
        HttpRequest httpRequest = new TestHttpRequest();
        httpRequest.headers().add(HttpHeaders.Names.ORIGIN, originValue);
        httpRequest.headers().add(HttpHeaders.Names.HOST, host);
        WriteCapturingChannel writeCapturingChannel = new WriteCapturingChannel();
        NettyHttpRequest request = new NettyHttpRequest(httpRequest, writeCapturingChannel);

        NettyHttpChannel channel = new NettyHttpChannel(httpServerTransport, request, null, randomBoolean());
        channel.sendResponse(new TestReponse());

        // get the response
        List<Object> writtenObjects = writeCapturingChannel.getWrittenObjects();
        assertThat(writtenObjects.size(), is(1));
        return (HttpResponse) writtenObjects.get(0);
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

        private ChannelBuffer content = ChannelBuffers.EMPTY_BUFFER;

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
            return content;
        }

        @Override
        public void setContent(ChannelBuffer content) {
            this.content = content;
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
