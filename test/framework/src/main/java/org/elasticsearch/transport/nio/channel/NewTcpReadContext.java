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

package org.elasticsearch.transport.nio.channel;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.nio.TcpReadHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NewTcpReadContext implements ReadContext {

    private final TcpReadHandler handler;
    private final NioSocketChannel channel;

    public NewTcpReadContext(NioSocketChannel channel, TcpReadHandler handler) {
        this.handler = handler;
        this.channel = channel;
    }

    @Override
    public int read() throws IOException {
        TcpFrameDecoder frameDecoder = new TcpFrameDecoder();
        byte[] bytes = new byte[frameDecoder.nextReadLength()];
        int bytesRead = channel.read(ByteBuffer.wrap(bytes));
        BytesReference message = frameDecoder.decode(new BytesArray(bytes));

        BytesRefIterator iterator = message.iterator();

        BytesRef next = iterator.next();

        return bytesRead;
    }
}
