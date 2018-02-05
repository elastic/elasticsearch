package org.elasticsearch.http.nio;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.nio.cors.Netty4CorsConfig;
import org.elasticsearch.http.nio.pipelining.HttpPipelinedRequest;
import org.elasticsearch.nio.NioSocketChannel;

public class NioHttpRequestHandler {

    //    private final NioHttpTransport transport;
    private final NamedXContentRegistry xContentRegistry;
    private final ThreadContext threadContext;
    private final Netty4CorsConfig corsConfig;
    private final boolean detailedErrorsEnabled;
    private final boolean httpPipeliningEnabled;
    private final boolean resetCookies;

    public NioHttpRequestHandler(Object transport, NamedXContentRegistry xContentRegistry, boolean detailedErrorsEnabled,
                                 ThreadContext threadContext, boolean httpPipeliningEnabled, Netty4CorsConfig corsConfig,
                                 boolean resetCookies) {
//        this.transport = transport;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.threadContext = threadContext;
        this.httpPipeliningEnabled = httpPipeliningEnabled;
        this.xContentRegistry = xContentRegistry;
        this.corsConfig = corsConfig;
        this.resetCookies = resetCookies;
    }

    public void handleMessage(NioSocketChannel channel, Channel nettyChannel, Object msg) {
        final FullHttpRequest request;
        final HttpPipelinedRequest pipelinedRequest;
        if (this.httpPipeliningEnabled && msg instanceof HttpPipelinedRequest) {
            pipelinedRequest = (HttpPipelinedRequest) msg;
            request = (FullHttpRequest) pipelinedRequest.last();
        } else {
            pipelinedRequest = null;
            request = (FullHttpRequest) msg;
        }

        final FullHttpRequest copy = new DefaultFullHttpRequest(request.protocolVersion(), request.method(), request.uri(),
            Unpooled.copiedBuffer(request.content()), request.headers(), request.trailingHeaders());
        final NioHttpRequest httpRequest = new NioHttpRequest(xContentRegistry, copy, channel);
        final NioHttpChannel httpChannel = new NioHttpChannel(httpRequest, channel, nettyChannel, pipelinedRequest, detailedErrorsEnabled,
            threadContext, corsConfig, resetCookies);

//        if (request.decoderResult().isSuccess()) {
//            transport.dispatchRequest(httpRequest, httpChannel);
//        } else {
//            assert request.decoderResult().isFailure();
//            transport.dispatchBadRequest(httpRequest, httpChannel, request.decoderResult().cause());
//        }
    }
}
