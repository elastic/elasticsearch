/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class Netty4HttpFlowControlHandlerTests extends ESTestCase {

    private EmbeddedChannel channel;
    private ReadSniffer readSniffer;
    private ReadCompleteCounter readCompletes;

    @Before
    public void initChannel() {
        channel = new EmbeddedChannel();
        channel.config().setAutoRead(false);
        readSniffer = new ReadSniffer();
        readCompletes = new ReadCompleteCounter();
        // readSniffer sits upstream so it counts the reads this handler forwards towards the network
        channel.pipeline().addLast(readSniffer, new Netty4HttpFlowControlHandler(), readCompletes);
    }

    @After
    public void closeChannel() {
        channel.close();
        channel.checkException();
    }

    /**
     * The invariant, over a random interleaving of socket reads and downstream reads: however many messages arrive
     * together, at most one is released per read, in arrival order, and none are lost.
     */
    public void testReleasesOneMessagePerReadInOrder() {
        var arrived = new ArrayList<HttpObject>();
        var released = new ArrayList<HttpObject>();

        for (int step = 0; step < between(50, 200); step++) {
            if (randomBoolean()) {
                var batch = randomBatch();
                arrived.addAll(batch);
                channel.writeInbound(batch.toArray());
            } else {
                channel.read();
            }
            collectOne(released);
        }

        while (released.size() < arrived.size()) {
            int before = released.size();
            channel.read();
            collectOne(released);
            assertEquals("a read with messages queued must release exactly one", before + 1, released.size());
        }

        assertEquals("no message may be lost", arrived.size(), released.size());
        for (int i = 0; i < arrived.size(); i++) {
            assertSame("messages must be released in arrival order", arrived.get(i), released.get(i));
        }
        releaseAll(arrived);
    }

    public void testDoesNotCombineContentRuns() {
        var first = randomContent(between(1, 64));
        var second = randomContent(between(1, 64));
        var last = randomLastContent(between(1, 64));

        channel.writeInbound(first, second, last);
        assertSame(first, readOne());
        assertSame(second, readOne());
        assertSame(last, readOne());
        releaseAll(List.of(first, second, last));
    }

    public void testSatisfiesReadArmedBeforeMessageArrives() {
        channel.read(); // the transport arms a read when it sets the channel up, before any message exists
        assertEquals(1, readSniffer.readCount);

        var content = randomContent(between(1, 64));
        channel.writeInbound(content);
        assertSame("an armed read must be satisfied as soon as a message arrives", content, channel.readInbound());
        assertNull("an armed read must be satisfied only once", channel.readInbound());
        content.release();
    }

    public void testRepeatedReadsAreDeduplicated() {
        for (int i = 0; i < between(2, 5); i++) {
            channel.read();
        }
        assertEquals("repeated reads with nothing to release must issue a single upstream read", 1, readSniffer.readCount);

        var first = randomContent(between(1, 64));
        var second = randomContent(between(1, 64));
        channel.writeInbound(first, second);
        assertSame(first, channel.readInbound());
        assertNull("repeated reads must not release more than one message", channel.readInbound());
        first.release(); // second is still queued, and closing the channel releases it
    }

    public void testFiresOneReadCompletePerReleasedMessage() {
        var content = randomContent(between(1, 64));
        var last = randomLastContent(between(1, 64));
        channel.writeInbound(content, last); // EmbeddedChannel fires channelReadComplete once the batch is written

        assertEquals("upstream read-completes must not reach handlers below", 0, readCompletes.count);
        readOne();
        assertEquals(1, readCompletes.count);
        readOne();
        assertEquals(2, readCompletes.count);
        releaseAll(List.of(content, last));
    }

    /**
     * A read cycle that produced nothing must be retried, or a wire chunk that decompressed to no output at all would
     * stall the stream.
     */
    public void testEmptyReadCycleReadsUpstreamAgain() {
        channel.read();
        assertEquals(1, readSniffer.readCount);

        channel.pipeline().fireChannelReadComplete();
        assertEquals("a read cycle that produced nothing must be retried", 2, readSniffer.readCount);
        assertEquals("nothing was released, so nothing may be signalled downstream", 0, readCompletes.count);
    }

    /**
     * A handler below may call {@code read()} from inside {@code channelRead}, as Netty4HttpContentSizeHandler does for
     * content it is dropping. That read must be honoured rather than swallowed by the flag the release just cleared.
     */
    public void testSynchronousDownstreamReadIsHonoured() {
        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ctx.fireChannelRead(msg);
                ctx.read();
            }
        });

        var batch = randomBatch();
        channel.writeInbound(batch.toArray());
        channel.read();

        for (HttpObject expected : batch) {
            assertSame("a read from within channelRead must release the next message", expected, channel.readInbound());
        }
        releaseAll(batch);
    }

    public void testReleasesQueuedContentOnClose() {
        var content = randomContent(between(1, 64));
        channel.writeInbound(content);
        assertEquals(1, content.refCnt());
        channel.close();
        assertEquals("queued content must be released when the channel closes", 0, content.refCnt());
    }

    private <T> T readOne() {
        T msg = channel.readInbound();
        if (msg == null) {
            channel.read();
            msg = channel.readInbound();
        }
        assertNull("must release at most one message per read", channel.readInbound());
        return msg;
    }

    private void collectOne(List<HttpObject> released) {
        HttpObject msg = channel.readInbound();
        if (msg != null) {
            released.add(msg);
        }
        assertNull("must release at most one message per read", channel.readInbound());
    }

    /**
     * One socket read's worth of decoded messages: a request followed by the chunks the decoder split its body into,
     * possibly for several pipelined requests.
     */
    private List<HttpObject> randomBatch() {
        var batch = new ArrayList<HttpObject>();
        for (int i = 0; i < between(1, 3); i++) {
            batch.add(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/" + i));
            int chunkCount = between(1, 5);
            for (int c = 0; c < chunkCount; c++) {
                batch.add(c == chunkCount - 1 ? randomLastContent(between(0, 64)) : randomContent(between(0, 64)));
            }
        }
        return batch;
    }

    private static void releaseAll(List<? extends HttpObject> msgs) {
        msgs.forEach(ReferenceCountUtil::release);
    }

    private HttpContent randomContent(int size) {
        return new DefaultHttpContent(Unpooled.wrappedBuffer(randomByteArrayOfLength(size)));
    }

    private LastHttpContent randomLastContent(int size) {
        return new DefaultLastHttpContent(Unpooled.wrappedBuffer(randomByteArrayOfLength(size)));
    }

    private static class ReadCompleteCounter extends ChannelInboundHandlerAdapter {
        int count;

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            count++;
            ctx.fireChannelReadComplete();
        }
    }
}
