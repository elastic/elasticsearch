/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.Matchers.sameInstance;

public class Netty4ContentDecompressorTests extends ESTestCase {

    public void testDecompressRequest() throws IOException {
        var chan = new EmbeddedChannel(new Netty4ContentDecompressor(), new HttpObjectAggregator(1024 * 1024));

        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            var data = randomByteArrayOfLength(512 * 1024);
            var zipped = Unpooled.buffer(data.length); // zipped version would about same size
            try (var zos = new GZIPOutputStream(new ByteBufOutputStream(zipped))) {
                zos.write(data);
                zos.flush();
            }

            var sendReq = new PipelinedHttpRequest(HttpMethod.POST, "/uri", 0);
            sendReq.headers().add(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.GZIP);
            chan.writeInbound(sendReq);
            chan.writeInbound(new PipelinedLastHttpContent(zipped, 0));

            var msg = chan.readInbound();
            assertNotNull(msg);
            var recvReq = (FullHttpRequest) msg;
            assertTrue(ByteBufUtil.equals(Unpooled.wrappedBuffer(data), recvReq.content()));
            recvReq.release();
        }
    }

    public void testSkipDecompression() {
        var chan = new EmbeddedChannel(new Netty4ContentDecompressor());

        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            var data = randomByteArrayOfLength(512 * 1024);
            var req = new PipelinedHttpRequest(HttpMethod.POST, "/uri", 0);
            var content = new PipelinedLastHttpContent(Unpooled.wrappedBuffer(data), 0);
            chan.writeInbound(req);
            chan.writeInbound(content);
            assertThat(chan.readInbound(), sameInstance(req));
            assertThat(chan.readInbound(), sameInstance(content));
        }
    }
}
