/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.sameInstance;

public class Netty4HttpRequestCreatorTests extends ESTestCase {
    public void testDecoderErrorSurfacedAsNettyInboundError() {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new Netty4HttpRequestCreator());
        // a request with a decoder error
        final DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        Exception cause = randomFrom(new ElasticsearchException("Boom"), new RuntimeException("Runtime boom"));
        request.setDecoderResult(DecoderResult.failure(cause));
        embeddedChannel.writeInbound(request);
        final Netty4HttpRequest nettyRequest = embeddedChannel.readInbound();
        assertThat(nettyRequest.getInboundException(), sameInstance(cause));
    }
}
