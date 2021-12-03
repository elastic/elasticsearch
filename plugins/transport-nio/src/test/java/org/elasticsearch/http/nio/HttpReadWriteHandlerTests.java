/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.CorsHandler;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.HttpPipelinedResponse;
import org.elasticsearch.http.HttpReadTimeoutException;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.TaskScheduler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HttpReadWriteHandlerTests extends ESTestCase {

    private HttpReadWriteHandler handler;
    private NioHttpChannel channel;
    private NioHttpServerTransport transport;
    private TaskScheduler taskScheduler;

    private final RequestEncoder requestEncoder = new RequestEncoder();
    private final ResponseDecoder responseDecoder = new ResponseDecoder();

    @Before
    public void setMocks() {
        transport = mock(NioHttpServerTransport.class);
        doAnswer(invocation -> {
            ((HttpRequest) invocation.getArguments()[0]).releaseAndCopy();
            return null;
        }).when(transport).incomingRequest(any(HttpRequest.class), any(HttpChannel.class));
        Settings settings = Settings.builder().put(SETTING_HTTP_MAX_CONTENT_LENGTH.getKey(), new ByteSizeValue(1024)).build();
        HttpHandlingSettings httpHandlingSettings = HttpHandlingSettings.fromSettings(settings);
        channel = mock(NioHttpChannel.class);
        taskScheduler = mock(TaskScheduler.class);

        handler = new HttpReadWriteHandler(channel, transport, httpHandlingSettings, taskScheduler, System::nanoTime);
        handler.channelActive();
    }

    public void testSuccessfulDecodeHttpRequest() throws IOException {
        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        io.netty.handler.codec.http.HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);

        ByteBuf buf = requestEncoder.encode(httpRequest);
        int slicePoint = randomInt(buf.writerIndex() - 1);
        ByteBuf slicedBuf = buf.retainedSlice(0, slicePoint);
        ByteBuf slicedBuf2 = buf.retainedSlice(slicePoint, buf.writerIndex() - slicePoint);
        try {
            handler.consumeReads(toChannelBuffer(slicedBuf));

            verify(transport, times(0)).incomingRequest(any(HttpRequest.class), any(NioHttpChannel.class));

            handler.consumeReads(toChannelBuffer(slicedBuf2));

            ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
            verify(transport).incomingRequest(requestCaptor.capture(), any(NioHttpChannel.class));

            HttpRequest nioHttpRequest = requestCaptor.getValue();
            assertEquals(HttpRequest.HttpVersion.HTTP_1_1, nioHttpRequest.protocolVersion());
            assertEquals(RestRequest.Method.GET, nioHttpRequest.method());
        } finally {
            handler.close();
            buf.release();
            slicedBuf.release();
            slicedBuf2.release();
        }
    }

    public void testDecodeHttpRequestError() throws IOException {
        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        io.netty.handler.codec.http.HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);

        ByteBuf buf = requestEncoder.encode(httpRequest);
        try {
            buf.setByte(0, ' ');
            buf.setByte(1, ' ');
            buf.setByte(2, ' ');

            handler.consumeReads(toChannelBuffer(buf));

            ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
            verify(transport).incomingRequest(requestCaptor.capture(), any(NioHttpChannel.class));

            assertNotNull(requestCaptor.getValue().getInboundException());
            assertTrue(requestCaptor.getValue().getInboundException() instanceof IllegalArgumentException);
        } finally {
            buf.release();
        }
    }

    public void testDecodeHttpRequestContentLengthToLongGeneratesOutboundMessage() throws IOException {
        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        io.netty.handler.codec.http.HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri, false);
        HttpUtil.setContentLength(httpRequest, 1025);
        HttpUtil.setKeepAlive(httpRequest, false);

        ByteBuf buf = requestEncoder.encode(httpRequest);
        try {
            handler.consumeReads(toChannelBuffer(buf));
        } finally {
            buf.release();
        }
        verify(transport, times(0)).incomingRequest(any(), any());

        List<FlushOperation> flushOperations = handler.pollFlushOperations();
        assertFalse(flushOperations.isEmpty());

        FlushOperation flushOperation = flushOperations.get(0);
        FullHttpResponse response = responseDecoder.decode(Unpooled.wrappedBuffer(flushOperation.getBuffersToWrite()));
        try {
            assertEquals(HttpVersion.HTTP_1_1, response.protocolVersion());
            assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());

            flushOperation.getListener().accept(null, null);
            // Since we have keep-alive set to false, we should close the channel after the response has been
            // flushed
            verify(channel).close();
        } finally {
            response.release();
        }
    }

    @SuppressWarnings("unchecked")
    public void testEncodeHttpResponse() throws IOException {
        prepareHandlerForResponse(handler);
        HttpPipelinedResponse httpResponse = emptyGetResponse(0);

        SocketChannelContext context = mock(SocketChannelContext.class);
        HttpWriteOperation writeOperation = new HttpWriteOperation(context, httpResponse, mock(BiConsumer.class));
        List<FlushOperation> flushOperations = handler.writeToBytes(writeOperation);
        FlushOperation operation = flushOperations.get(0);
        FullHttpResponse response = responseDecoder.decode(Unpooled.wrappedBuffer(operation.getBuffersToWrite()));
        ((ChannelPromise) operation.getListener()).setSuccess();
        try {
            assertEquals(HttpResponseStatus.OK, response.status());
            assertEquals(HttpVersion.HTTP_1_1, response.protocolVersion());
        } finally {
            response.release();
        }
    }

    @SuppressWarnings("unchecked")
    public void testReadTimeout() throws IOException {
        TimeValue timeValue = TimeValue.timeValueMillis(500);
        Settings settings = Settings.builder().put(SETTING_HTTP_READ_TIMEOUT.getKey(), timeValue).build();
        HttpHandlingSettings httpHandlingSettings = HttpHandlingSettings.fromSettings(settings);

        CorsHandler corsHandler = CorsHandler.disabled();
        TaskScheduler realScheduler = new TaskScheduler();

        Iterator<Integer> timeValues = Arrays.asList(0, 2, 4, 6, 8).iterator();
        handler = new HttpReadWriteHandler(channel, transport, httpHandlingSettings, realScheduler, timeValues::next);
        handler.channelActive();

        prepareHandlerForResponse(handler);
        SocketChannelContext context = mock(SocketChannelContext.class);
        HttpWriteOperation writeOperation0 = new HttpWriteOperation(context, emptyGetResponse(0), mock(BiConsumer.class));
        ((ChannelPromise) handler.writeToBytes(writeOperation0).get(0).getListener()).setSuccess();

        realScheduler.pollTask(timeValue.getNanos() + 1).run();
        // There was a read. Do not close.
        verify(transport, times(0)).onException(eq(channel), any(HttpReadTimeoutException.class));

        prepareHandlerForResponse(handler);
        prepareHandlerForResponse(handler);

        realScheduler.pollTask(timeValue.getNanos() + 3).run();
        // There was a read. Do not close.
        verify(transport, times(0)).onException(eq(channel), any(HttpReadTimeoutException.class));

        HttpWriteOperation writeOperation1 = new HttpWriteOperation(context, emptyGetResponse(1), mock(BiConsumer.class));
        ((ChannelPromise) handler.writeToBytes(writeOperation1).get(0).getListener()).setSuccess();

        realScheduler.pollTask(timeValue.getNanos() + 5).run();
        // There has not been a read, however there is still an inflight request. Do not close.
        verify(transport, times(0)).onException(eq(channel), any(HttpReadTimeoutException.class));

        HttpWriteOperation writeOperation2 = new HttpWriteOperation(context, emptyGetResponse(2), mock(BiConsumer.class));
        ((ChannelPromise) handler.writeToBytes(writeOperation2).get(0).getListener()).setSuccess();

        realScheduler.pollTask(timeValue.getNanos() + 7).run();
        // No reads and no inflight requests, close
        verify(transport, times(1)).onException(eq(channel), any(HttpReadTimeoutException.class));
        assertNull(realScheduler.pollTask(timeValue.getNanos() + 9));
    }

    private static HttpPipelinedResponse emptyGetResponse(int sequence) {
        DefaultFullHttpRequest nettyRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        HttpPipelinedRequest httpRequest = new HttpPipelinedRequest(sequence, new NioHttpRequest(nettyRequest));
        HttpPipelinedResponse httpResponse = httpRequest.createResponse(RestStatus.OK, BytesArray.EMPTY);
        httpResponse.addHeader(HttpHeaderNames.CONTENT_LENGTH.toString(), "0");
        return httpResponse;
    }

    private void prepareHandlerForResponse(HttpReadWriteHandler readWriteHandler) throws IOException {
        HttpMethod method = randomBoolean() ? HttpMethod.GET : HttpMethod.HEAD;
        HttpVersion version = randomBoolean() ? HttpVersion.HTTP_1_0 : HttpVersion.HTTP_1_1;
        String uri = "http://localhost:9090/" + randomAlphaOfLength(8);

        io.netty.handler.codec.http.HttpRequest request = new DefaultFullHttpRequest(version, method, uri);
        ByteBuf buf = requestEncoder.encode(request);
        try {
            readWriteHandler.consumeReads(toChannelBuffer(buf));
        } finally {
            buf.release();
        }

        ArgumentCaptor<HttpPipelinedRequest> requestCaptor = ArgumentCaptor.forClass(HttpPipelinedRequest.class);
        verify(transport, atLeastOnce()).incomingRequest(requestCaptor.capture(), any(HttpChannel.class));

        HttpRequest httpRequest = requestCaptor.getValue();
        assertNotNull(httpRequest);
        assertEquals(method.name(), httpRequest.method().name());
        if (version == HttpVersion.HTTP_1_1) {
            assertEquals(HttpRequest.HttpVersion.HTTP_1_1, httpRequest.protocolVersion());
        } else {
            assertEquals(HttpRequest.HttpVersion.HTTP_1_0, httpRequest.protocolVersion());
        }
        assertEquals(httpRequest.uri(), uri);
    }

    private InboundChannelBuffer toChannelBuffer(ByteBuf buf) {
        InboundChannelBuffer buffer = InboundChannelBuffer.allocatingInstance();
        int readableBytes = buf.readableBytes();
        buffer.ensureCapacity(readableBytes);
        int bytesWritten = 0;
        ByteBuffer[] byteBuffers = buffer.sliceBuffersTo(readableBytes);
        int i = 0;
        while (bytesWritten != readableBytes) {
            ByteBuffer byteBuffer = byteBuffers[i++];
            int initialRemaining = byteBuffer.remaining();
            buf.readBytes(byteBuffer);
            bytesWritten += initialRemaining - byteBuffer.remaining();
        }
        buffer.incrementIndex(bytesWritten);
        return buffer;
    }

    private static final int MAX = 16 * 1024 * 1024;

    private static class RequestEncoder {

        private final EmbeddedChannel requestEncoder = new EmbeddedChannel(new HttpRequestEncoder(), new HttpObjectAggregator(MAX));

        private ByteBuf encode(io.netty.handler.codec.http.HttpRequest httpRequest) {
            requestEncoder.writeOutbound(httpRequest);
            return requestEncoder.readOutbound();
        }
    }

    private static class ResponseDecoder {

        private final EmbeddedChannel responseDecoder = new EmbeddedChannel(new HttpResponseDecoder(), new HttpObjectAggregator(MAX));

        private FullHttpResponse decode(ByteBuf response) {
            responseDecoder.writeInbound(response);
            return responseDecoder.readInbound();
        }
    }
}
