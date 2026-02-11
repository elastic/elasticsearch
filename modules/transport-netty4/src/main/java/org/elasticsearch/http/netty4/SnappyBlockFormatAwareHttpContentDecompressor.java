/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;

import java.util.List;

import static io.netty.handler.codec.http.HttpHeaderValues.SNAPPY;

/**
 * A custom {@link HttpContentDecompressor} that is aware of the Prometheus remote write API
 * and can handle Snappy block format compression for requests to that API.
 * This is necessary because the Prometheus remote write API uses Snappy block format compression,
 * which is not supported by the default Netty Snappy decoder which expects the framed format.
 */
final class SnappyBlockFormatAwareHttpContentDecompressor extends HttpContentDecompressor {

    private static final String PROMETHEUS_REMOTE_WRITE_VERSION_HEADER = "X-Prometheus-Remote-Write-Version";
    private static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
    private static final String PROMETHEUS_PATH_PREFIX = "/_prometheus/";
    private static final String PROMETHEUS_PATH_PREFIX_WITHOUT_LEADING_SLASH = "_prometheus/";
    private static final int MAX_SNAPPY_BLOCK_UNCOMPRESSED_SIZE = 32 * 1024 * 1024;

    private boolean prometheusRemoteWriteRequest;

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
        if (msg instanceof HttpRequest httpRequest) {
            prometheusRemoteWriteRequest = isPrometheusRemoteWriteRequest(httpRequest.method(), httpRequest.uri(), httpRequest.headers());
        }
        super.decode(ctx, msg, out);
    }

    @Override
    protected EmbeddedChannel newContentDecoder(String contentEncoding) throws Exception {
        if (SNAPPY.contentEqualsIgnoreCase(contentEncoding) && prometheusRemoteWriteRequest) {
            return new EmbeddedChannel(
                ctx.channel().id(),
                ctx.channel().metadata().hasDisconnect(),
                ctx.channel().config(),
                new SnappyBlockDecoder(MAX_SNAPPY_BLOCK_UNCOMPRESSED_SIZE)
            );
        }
        return super.newContentDecoder(contentEncoding);
    }

    private boolean isPrometheusRemoteWriteRequest(HttpMethod requestMethod, String uri, HttpHeaders headers) {
        return requestMethod == HttpMethod.POST
            && uri != null
            && (uri.startsWith(PROMETHEUS_PATH_PREFIX) || uri.startsWith(PROMETHEUS_PATH_PREFIX_WITHOUT_LEADING_SLASH))
            && headers.contains(PROMETHEUS_REMOTE_WRITE_VERSION_HEADER)
            && isPrometheusContentType(headers.get(HttpHeaderNames.CONTENT_TYPE));
    }

    private static boolean isPrometheusContentType(String contentTypeHeader) {
        if (contentTypeHeader == null) {
            return false;
        }
        int semicolonIndex = contentTypeHeader.indexOf(';');
        String mediaType = semicolonIndex >= 0 ? contentTypeHeader.substring(0, semicolonIndex) : contentTypeHeader;
        return PROTOBUF_CONTENT_TYPE.equalsIgnoreCase(mediaType.trim());
    }

}
