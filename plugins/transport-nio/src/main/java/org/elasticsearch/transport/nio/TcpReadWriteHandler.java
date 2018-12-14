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

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.nio.BytesWriteHandler;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.transport.TcpTransport;

import java.io.IOException;

public class TcpReadWriteHandler extends BytesWriteHandler {

    private final NioTcpChannel channel;
    private final TcpTransport transport;

    public TcpReadWriteHandler(NioTcpChannel channel, TcpTransport transport) {
        this.channel = channel;
        this.transport = transport;
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        BytesReference bytesReference = BytesReference.fromByteBuffers(channelBuffer.sliceBuffersTo(channelBuffer.getIndex()));
        return transport.consumeNetworkReads(channel, bytesReference);
    }
}
