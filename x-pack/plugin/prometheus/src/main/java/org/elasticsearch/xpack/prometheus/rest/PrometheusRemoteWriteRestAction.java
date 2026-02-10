/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.compression.Snappy;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST action for Prometheus remote write requests.
 * <p>
 * This handler bypasses the HTTP pipeline's automatic decompression because
 * the <a href="https://prometheus.io/docs/concepts/remote_write_spec/">Prometheus remote write spec</a>
 * mandates snappy <em>block</em> format ({@code Content-Encoding: snappy}), which is incompatible
 * with the snappy <em>framed</em> format that Netty's built-in decompressor implements.
 * Decompression is performed here using Netty's {@link Snappy} class.
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusRemoteWriteRestAction extends BaseRestHandler {

    static final String SNAPPY_ENCODING = "snappy";

    /**
     * Maximum allowed uncompressed payload size (32 MB). Protects against resource exhaustion
     * from malformed or malicious requests since the snappy block format does not support streaming.
     * The size of this limit is in line with one used by Prometheus itself.
     */
    static final int MAX_UNCOMPRESSED_SIZE = 32 * 1024 * 1024;

    @Override
    public String getName() {
        return "prometheus_remote_write_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_prometheus/api/v1/write"));
    }

    @Override
    public boolean handlesContentDecoding() {
        return true;
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        return request.getXContentType() == null
            && request.getParsedContentType().mediaTypeWithoutParameters().equals("application/x-protobuf");
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (request.hasContent()) {
            BytesReference content = decompressContent(request);
            var transportRequest = new PrometheusRemoteWriteTransportAction.RemoteWriteRequest(content);
            return channel -> client.execute(
                PrometheusRemoteWriteTransportAction.TYPE,
                transportRequest,
                new RestResponseListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(PrometheusRemoteWriteTransportAction.RemoteWriteResponse r) {
                        if (r.getMessage() != null) {
                            return new RestResponse(r.getStatus(), r.getMessage());
                        }
                        return new RestResponse(r.getStatus(), RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
                    }
                }
            );
        }

        return channel -> channel.sendResponse(new RestResponse(RestStatus.NO_CONTENT, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
    }

    /**
     * Decompresses the request content if {@code Content-Encoding: snappy} is present.
     * Uses snappy block format, as required by the Prometheus remote write specification.
     */
    static BytesReference decompressContent(RestRequest request) {
        // The remote write spec requires snappy compression in block format
        if (SNAPPY_ENCODING.equalsIgnoreCase(request.header("Content-Encoding")) == false) {
            throw new IllegalArgumentException("Prometheus remote write requests must use 'Content-Encoding: snappy' compression");
        }
        byte[] compressed = BytesReference.toBytes(request.content());
        ByteBuf input = Unpooled.wrappedBuffer(compressed);
        ByteBuf output = Unpooled.buffer();
        try {
            int uncompressedLength = readUncompressedLength(input);
            if (uncompressedLength > MAX_UNCOMPRESSED_SIZE) {
                throw new IllegalArgumentException(
                    "snappy uncompressed size [" + uncompressedLength + "] exceeds maximum allowed size [" + MAX_UNCOMPRESSED_SIZE + "]"
                );
            }
            input.readerIndex(0);
            new Snappy().decode(input, output);
            byte[] result = new byte[output.readableBytes()];
            output.readBytes(result);
            return new BytesArray(result);
        } finally {
            input.release();
            output.release();
        }
    }

    /**
     * Reads the uncompressed length from a snappy block-format preamble (varint-encoded).
     */
    static int readUncompressedLength(ByteBuf in) {
        int result = 0;
        int shift = 0;
        while (in.isReadable()) {
            byte b = in.readByte();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift >= 32) {
                throw new IllegalArgumentException("varint too long in snappy preamble");
            }
        }
        throw new IllegalArgumentException("truncated varint in snappy preamble");
    }
}
