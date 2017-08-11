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

package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.transport.nio.NetworkBytesReference;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.ReadContext;

import java.io.IOException;
import java.util.LinkedList;

public class HttpReadContext implements ReadContext {

    private final NioSocketChannel channel;
    private final EmbeddedChannel nettyPipelineAdaptor;
    private final LinkedList<NetworkBytesReference> references = new LinkedList<>();
    private final NioHttpRequestHandler requestHandler;

    public HttpReadContext(NioSocketChannel channel, NioHttpNettyAdaptor adaptor, NioHttpRequestHandler requestHandler) {
        this.channel = channel;
        this.requestHandler = requestHandler;
        this.nettyPipelineAdaptor = adaptor.getAdaptor(channel);
    }

    @Override
    public int read() throws IOException {
        NetworkBytesReference last = references.peekLast();
        if (last == null || last.hasWriteRemaining() == false) {
            this.references.add(NetworkBytesReference.wrap(new BytesArray(new byte[DEFAULT_READ_LENGTH])));
        }

        int bytesRead = channel.read(references.getLast());

        if (bytesRead == -1) {
            return bytesRead;
        }

        int size = references.size();

        ByteBuf inboundBytes = toByteBuf(size);

        nettyPipelineAdaptor.writeInbound(inboundBytes);

        Object msg;
        while ((msg = nettyPipelineAdaptor.readInbound()) != null) {
            requestHandler.handleMessage(channel, nettyPipelineAdaptor, msg);
        }

        return bytesRead;
    }

    private ByteBuf toByteBuf(int size) {
        if (size == 1) {
            return Unpooled.wrappedBuffer(references.getFirst().getReadByteBuffer());
        } else {
            CompositeByteBuf byteBuf = Unpooled.compositeBuffer(size);
            for (NetworkBytesReference reference : references) {
                // TODO: Do I need to increment reader indexes?
                byteBuf.addComponent(true, Unpooled.wrappedBuffer(reference.getReadByteBuffer()));
            }
            return byteBuf;
        }
    }
}
