/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

public class FlushReadyWrite extends FlushOperation implements WriteOperation {

    private final SocketChannelContext channelContext;
    private final ByteBuffer[] buffers;

    public FlushReadyWrite(SocketChannelContext channelContext, ByteBuffer[] buffers, BiConsumer<Void, Exception> listener) {
        super(buffers, listener);
        this.channelContext = channelContext;
        this.buffers = buffers;
    }

    @Override
    public SocketChannelContext getChannel() {
        return channelContext;
    }

    @Override
    public ByteBuffer[] getObject() {
        return buffers;
    }
}
