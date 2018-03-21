package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.nio.cors.Netty4CorsConfigBuilder;
import org.elasticsearch.http.nio.pipelining.HttpPipelinedRequest;
import org.elasticsearch.http.nio.pipelining.HttpPipelinedResponse;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Queue;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NioHttpNettyAdaptorTests extends ESTestCase {

    private BiConsumer<NioSocketChannel, Throwable> exceptionHandler;
    private NioHttpNettyAdaptor adaptor;
    private NioSocketChannel nioSocketChannel;

    private final RequestEncoder requestEncoder = new RequestEncoder();
    private final ResponseDecoder responseDecoder = new ResponseDecoder();

    @Before
    @SuppressWarnings("unchecked")
    public void setMocks() {
        exceptionHandler = mock(BiConsumer.class);

        adaptor = new NioHttpNettyAdaptor(logger, Settings.EMPTY, exceptionHandler, Netty4CorsConfigBuilder.forAnyOrigin().build(), 1024);
        nioSocketChannel = mock(NioSocketChannel.class);
    }

    @SuppressWarnings("unchecked")
    public void testCloseAdaptorSchedulesRealChannelForClose() {
        NioSocketChannel channel = mock(NioSocketChannel.class);
        NettyChannelAdaptor channelAdaptor = adaptor.getAdaptor(channel);
        ArgumentCaptor<BiConsumer> captor = ArgumentCaptor.forClass(BiConsumer.class);

        ChannelFuture nettyFuture = channelAdaptor.close();
        verify(channel).close();
        verify(channel).addCloseListener(captor.capture());

        BiConsumer listener = captor.getValue();
        assertFalse(nettyFuture.isDone());
        if (randomBoolean()) {
            listener.accept(null, null);
            assertTrue(nettyFuture.isSuccess());
        } else {
            IOException e = new IOException();
            listener.accept(null, e);
            assertFalse(nettyFuture.isSuccess());
            assertSame(e, nettyFuture.cause());
        }

        assertTrue(nettyFuture.isDone());
    }

    public void testSuccessfulDecodeHttpRequest() {
        NettyChannelAdaptor channelAdaptor = adaptor.getAdaptor(nioSocketChannel);

        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);

        ByteBuf buf = requestEncoder.encode(httpRequest);
        int slicePoint = randomInt(buf.writerIndex() - 1);

        ByteBuf slicedBuf = buf.retainedSlice(0, slicePoint);
        Queue<Object> messages = channelAdaptor.decode(slicedBuf);

        assertTrue(messages.isEmpty());

        messages = channelAdaptor.decode(buf.retainedSlice(slicePoint, buf.writerIndex() - slicePoint));
        HttpPipelinedRequest decodedRequest = (HttpPipelinedRequest) messages.poll();

        FullHttpRequest fullHttpRequest = (FullHttpRequest) decodedRequest.last();
        assertEquals(httpRequest.protocolVersion(), fullHttpRequest.protocolVersion());
        assertEquals(httpRequest.method(), fullHttpRequest.method());
    }

    public void testDecodeHttpRequestError() {
        NettyChannelAdaptor channelAdaptor = adaptor.getAdaptor(nioSocketChannel);

        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);

        ByteBuf buf = requestEncoder.encode(httpRequest);
        buf.setByte(0, ' ');
        buf.setByte(1, ' ');
        buf.setByte(2, ' ');

        HttpPipelinedRequest decodedRequest = (HttpPipelinedRequest) channelAdaptor.decode(buf.retainedDuplicate()).poll();

        FullHttpRequest fullHttpRequest = (FullHttpRequest) decodedRequest.last();
        DecoderResult decoderResult = fullHttpRequest.decoderResult();
        assertTrue(decoderResult.isFailure());
        assertTrue(decoderResult.cause() instanceof IllegalArgumentException);
    }

    public void testDecodeHttpRequestContentLengthToLongGeneratesOutboundMessage() {
        NettyChannelAdaptor channelAdaptor = adaptor.getAdaptor(nioSocketChannel);

        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri, false);
        HttpUtil.setContentLength(httpRequest, 1025);

        ByteBuf buf = requestEncoder.encode(httpRequest);

        channelAdaptor.writeInbound(buf.retainedDuplicate());

        assertTrue(channelAdaptor.decode(buf.retainedDuplicate()).isEmpty());

        FlushOperation message = channelAdaptor.pollBytes();

        assertFalse(((ChannelPromise) message.getListener()).isDone());

        HttpResponse response = responseDecoder.decode(Unpooled.wrappedBuffer(message.getBuffersToWrite()));
        assertEquals(HttpVersion.HTTP_1_1, response.protocolVersion());
        assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());
    }

    public void testEncodeHttpResponse() {
        NettyChannelAdaptor channelAdaptor = adaptor.getAdaptor(nioSocketChannel);

        prepareAdaptorForResponse(channelAdaptor);

        HttpResponse defaultFullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

        channelAdaptor.writeOutbound(defaultFullHttpResponse);
        FlushOperation encodedMessage = channelAdaptor.pollBytes();

        HttpResponse response = responseDecoder.decode(Unpooled.wrappedBuffer(encodedMessage.getBuffersToWrite()));

        assertEquals(HttpResponseStatus.OK, response.status());
        assertEquals(HttpVersion.HTTP_1_1, response.protocolVersion());
    }

//    public void testEncodedMessageIsReleasedWhenPromiseCompleted() {
//        NettyChannelAdaptor channelAdaptor = adaptor.getAdaptor(nioSocketChannel);
//
//        prepareAdaptorForResponse(channelAdaptor);
//
//        HttpResponse defaultFullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
//
//        channelAdaptor.writeOutbound(defaultFullHttpResponse);
//        BytesWriteOperation encodedMessage = channelAdaptor.pollBytes();
//
//        ByteBufBytesReference reference = (ByteBufBytesReference) encodedMessage.getBuffersToWrite();
//
//        ByteBuf byteBuf = reference.toByteBuf();
//        assertEquals(1, byteBuf.refCnt());
//        byteBuf.retain();
//        assertEquals(2, byteBuf.refCnt());
//
//        if (randomBoolean()) {
//            encodedMessage.v2().setSuccess();
//        } else {
//            encodedMessage.v2().setFailure(new ClosedChannelException());
//        }
//
//        assertEquals(1, byteBuf.refCnt());
//        assertTrue(byteBuf.release());
//    }

    public void testResponsesAreClearedOnClose() {
        adaptor = new NioHttpNettyAdaptor(logger, Settings.EMPTY, exceptionHandler, Netty4CorsConfigBuilder.forAnyOrigin().build(), 1024);
        NettyChannelAdaptor channelAdaptor = adaptor.getAdaptor(nioSocketChannel);

        prepareAdaptorForResponse(channelAdaptor);
        HttpPipelinedRequest pipelinedRequest2 = prepareAdaptorForResponse(channelAdaptor);

        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        ChannelPromise writePromise = channelAdaptor.newPromise();
        HttpPipelinedResponse pipelinedResponse = pipelinedRequest2.createHttpResponse(httpResponse, writePromise);

        channelAdaptor.write(pipelinedResponse, writePromise);
        assertNull(channelAdaptor.pollBytes());
        assertFalse(writePromise.isDone());

        ChannelFuture close = channelAdaptor.close();

        assertFalse(close.isDone());
        assertTrue(writePromise.isDone());
        assertTrue(writePromise.cause() instanceof ClosedChannelException);
    }

    private HttpPipelinedRequest prepareAdaptorForResponse(NettyChannelAdaptor adaptor) {
        HttpMethod method = HttpMethod.GET;
        HttpVersion version = HttpVersion.HTTP_1_1;
        String uri = "http://localhost:9090/" + randomAlphaOfLength(8);

        HttpRequest request = new DefaultFullHttpRequest(version, method, uri);
        ByteBuf buf = requestEncoder.encode(request);

        HttpPipelinedRequest pipelinedRequest = (HttpPipelinedRequest) adaptor.decode(buf).poll();
        FullHttpRequest requestParsed = (FullHttpRequest) pipelinedRequest.last();
        assertNotNull(requestParsed);
        assertEquals(requestParsed.method(), method);
        assertEquals(requestParsed.protocolVersion(), version);
        assertEquals(requestParsed.uri(), uri);
        return pipelinedRequest;
    }

    private static class RequestEncoder {

        private final EmbeddedChannel requestEncoder = new EmbeddedChannel(new HttpRequestEncoder());

        private ByteBuf encode(HttpRequest httpRequest) {
            requestEncoder.writeOutbound(httpRequest);
            return requestEncoder.readOutbound();
        }
    }

    private static class ResponseDecoder {

        private final EmbeddedChannel responseDecoder = new EmbeddedChannel(new HttpResponseDecoder());

        private HttpResponse decode(ByteBuf response) {
            responseDecoder.writeInbound(response);
            return responseDecoder.readInbound();
        }
    }
}
