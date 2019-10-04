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

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;

public class InboundDecoder implements Closeable {

    private static final ReleasableBytesReference END_CONTENT = new ReleasableBytesReference(BytesArray.EMPTY, () -> {});

    private final InboundAggregator aggregator;
    private final PageCacheRecycler recycler;
    private TransportDecompressor decompressor;
    private int networkMessageSize = -1;
    private int bytesConsumed = 0;

    public InboundDecoder(InboundAggregator aggregator) {
        this(aggregator, PageCacheRecycler.NON_RECYCLING_INSTANCE);
    }

    public InboundDecoder(InboundAggregator aggregator, PageCacheRecycler recycler) {
        this.aggregator = aggregator;
        this.recycler = recycler;
    }

    public int handle(ReleasableBytesReference releasable) throws IOException {
        if (isOnHeader()) {
            int expectedLength = TcpTransport.readMessageLength(releasable.getReference());
            if (expectedLength == -1) {
                releasable.close();
                return 0;
            } else if (expectedLength == 0) {
                aggregator.pingReceived(releasable.getReference().slice(0, 6));
                releasable.close();
                return 6;
            } else {
                if (releasable.getReference().length() < TcpHeader.HEADER_SIZE) {
                    releasable.close();
                    return 0;
                } else {
                    networkMessageSize = expectedLength;
                    Header header = parseHeader(networkMessageSize, releasable.getReference());
                    bytesConsumed += TcpHeader.HEADER_SIZE - 6;
                    if (header.isCompressed()) {
                        decompressor = new TransportDecompressor(recycler);
                    }
                    aggregator.headerReceived(header);
                    return TcpHeader.HEADER_SIZE;
                }
            }
        } else {
            int bytesToConsume = Math.min(releasable.getReference().length(), networkMessageSize - bytesConsumed);
            bytesConsumed += bytesToConsume;
            ReleasableBytesReference content;
            if (isDone()) {
                BytesReference sliced = releasable.getReference().slice(0, bytesToConsume);
                content = new ReleasableBytesReference(sliced, releasable.getReleasable());
            } else {
                content = releasable;
            }
            if (decompressor != null) {
                decompressor.decompress(content.getReference());
                releasable.close();
                ReleasableBytesReference decompressed;
                while ((decompressed = decompressor.pollDecompressedPage()) != null) {
                    aggregator.contentReceived(decompressed);
                }
            } else {
                aggregator.contentReceived(content);
            }
            if (isDone()) {
                decompressor = null;
                networkMessageSize = -1;
                bytesConsumed = 0;
                aggregator.contentReceived(END_CONTENT);
            }

            return bytesToConsume;
        }
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(decompressor);
    }

    private boolean isDone() {
        return bytesConsumed == networkMessageSize;
    }

    private Header parseHeader(int networkMessageSize, BytesReference bytesReference) throws IOException {
        try (StreamInput streamInput = bytesReference.streamInput()) {
            streamInput.skip(6);
            long requestId = streamInput.readLong();
            byte status = streamInput.readByte();
            Version remoteVersion = Version.fromId(streamInput.readInt());
            return new Header(networkMessageSize, requestId, status, remoteVersion);
        }
    }

    private boolean isOnHeader() {
        return networkMessageSize == -1;
    }
}
