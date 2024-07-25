/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.sameInstance;

public class Netty4HttpAggregatorTests extends ESTestCase {

    public void testAggregateParts() {
        var chan = new EmbeddedChannel(new Netty4HttpAggregator(1024 * 1024));
        var reqNum = randomIntBetween(1, 100);
        var wantData = new ArrayList<ByteBuf>();

        for (var sequence = 0; sequence < reqNum; sequence++) {
            chan.writeInbound(new PipelinedHttpRequest(HttpMethod.GET, "/uri", sequence));
            var data = Unpooled.buffer(128);
            for (var part = 0; part < randomInt(10); part++) {
                var chunk = randomByteArrayOfLength(128);
                data.writeBytes(chunk);
                chan.writeInbound(new PipelinedHttpContent(Unpooled.wrappedBuffer(chunk), sequence));
            }
            if (randomBoolean()) {
                chan.writeInbound(new PipelinedLastHttpContent(LastHttpContent.EMPTY_LAST_CONTENT, sequence));
            } else {
                var last = randomByteArrayOfLength(128);
                data.writeBytes(last);
                chan.writeInbound(new PipelinedLastHttpContent(Unpooled.wrappedBuffer(last), sequence));
            }
            wantData.add(data);
        }

        assertEquals("should aggregate all requests", reqNum, chan.inboundMessages().size());
        for (int sequence = 0; sequence < reqNum; sequence++) {
            var fullReq = (PipelinedFullHttpRequest) chan.inboundMessages().poll();
            assertEquals("sequence must match", fullReq.sequence(), sequence);
            assertTrue("content must match", ByteBufUtil.equals(wantData.get(sequence), fullReq.content()));
        }
    }

    public void testSkipAggregationByPredicate() {
        Predicate<PipelinedHttpRequest> skipBulkApi = (req) -> req.uri().equals("_bulk") == false;
        var chan = new EmbeddedChannel(new Netty4HttpAggregator(1024, skipBulkApi));

        var req = new PipelinedHttpRequest(HttpMethod.POST, "_bulk", 0);
        var content = new PipelinedLastHttpContent(LastHttpContent.EMPTY_LAST_CONTENT, 0);
        chan.writeInbound(req, content);

        assertThat(chan.readInbound(), sameInstance(req));
        assertThat(chan.readInbound(), sameInstance(content));
    }

}
