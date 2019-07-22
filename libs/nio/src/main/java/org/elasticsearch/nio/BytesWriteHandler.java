/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
