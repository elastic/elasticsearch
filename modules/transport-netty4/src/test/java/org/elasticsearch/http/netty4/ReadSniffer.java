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
import io.netty.channel.ChannelOutboundHandlerAdapter;

/**
 * Sniffs channel reads, helps detect missing or unexpected ones.
 * <pre>
 *     {@code
 *     chan = new EmbeddedChannel();
 *     chan.config().setAutoRead(false);
 *     readSniffer = new ReadSniffer();
 *     chan.pipeline().addLast(readSniffer, ...otherHandlers);
 *     ...
 *     // run test
 *     ...
 *     assertEquals("unexpected read", 0, readSniffer.readCnt)
 *     // or
 *     assertEquals("exact number of reads", 2, readSniffer.readCnt)
 *     }
 * </pre>
 *
 */
public class ReadSniffer extends ChannelOutboundHandlerAdapter {

    int readCount;

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        readCount++;
        super.read(ctx);
    }
}
