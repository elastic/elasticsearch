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
