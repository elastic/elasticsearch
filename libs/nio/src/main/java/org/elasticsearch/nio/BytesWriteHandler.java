/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

public abstract class BytesWriteHandler implements NioChannelHandler {

    private static final List<FlushOperation> EMPTY_LIST = Collections.emptyList();

    public WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener) {
        assert message instanceof ByteBuffer[] : "This channel only supports messages that are of type: " + ByteBuffer[].class
            + ". Found type: " + message.getClass() + ".";
        return new FlushReadyWrite(context, (ByteBuffer[]) message, listener);
    }

    @Override
    public void channelActive() {}

    @Override
    public List<FlushOperation> writeToBytes(WriteOperation writeOperation) {
        assert writeOperation instanceof FlushReadyWrite : "Write operation must be flush ready";
        return Collections.singletonList((FlushReadyWrite) writeOperation);
    }

    @Override
    public List<FlushOperation> pollFlushOperations() {
        return EMPTY_LIST;
    }

    @Override
    public boolean closeNow() {
        return false;
    }

    @Override
    public void close() {}
}
