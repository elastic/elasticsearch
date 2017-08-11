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
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.transport.nio.ByteWriteOperation;
import org.elasticsearch.transport.nio.NetworkBytesReference;
import org.elasticsearch.transport.nio.WriteOperation;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

public class HttpWriteOperation extends WriteOperation {

    private final ESEmbeddedChannel nettyChannel;
    private NetworkBytesReference[] byteReferences;
    private HttpResponse httpResponse;

    public HttpWriteOperation(NioSocketChannel channel, ESEmbeddedChannel nettyChannel, Object object, ChannelPromise promise) {
        super(channel, new ActionListener<NioChannel>() {
            @Override
            public void onResponse(NioChannel nioChannel) {
                promise.setSuccess();
            }

            @Override
            public void onFailure(Exception e) {
                promise.setFailure(e);
            }
        });
        this.nettyChannel = nettyChannel;
        if (object instanceof BytesReference) {
            byteReferences = ByteWriteOperation.toArray((BytesReference) object);
        } else {
            httpResponse = (HttpResponse) object;
        }
    }

    @Override
    public NetworkBytesReference[] getByteReferences() {
        if (byteReferences == null) {
            assert channel.getSelector().isOnCurrentThread() : "must be on selector thread to serialize http response";
            nettyChannel.writeOutbound(httpResponse);
            // I do not think this works with pipelined work
            Tuple<BytesReference, ChannelPromise> t = nettyChannel.getMessage();
            ChannelPromise serializationPromise = t.v2();
            if (serializationPromise.isDone() && serializationPromise.isSuccess() == false) {
                throw new ElasticsearchException(serializationPromise.cause());
            }
            byteReferences = ByteWriteOperation.toArray(t.v1());
        }
        return byteReferences;
    }
}
