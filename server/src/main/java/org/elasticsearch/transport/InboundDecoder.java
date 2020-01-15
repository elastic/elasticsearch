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
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;

public class InboundDecoder implements Releasable {

    static final ReleasableBytesReference END_CONTENT = new ReleasableBytesReference(BytesArray.EMPTY, () -> {
    });

    private final InboundAggregator aggregator;
    private final PageCacheRecycler recycler;
    private TransportDecompressor decompressor;
    private int totalNetworkSize = -1;
    private int bytesConsumed = 0;

    public InboundDecoder(InboundAggregator aggregator) {
        this(aggregator, PageCacheRecycler.NON_RECYCLING_INSTANCE);
    }

    public InboundDecoder(InboundAggregator aggregator, PageCacheRecycler recycler) {
        this.aggregator = aggregator;
        this.recycler = recycler;
    }

    public int handle(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        if (isOnHeader()) {
            int messageLength = TcpTransport.readMessageLength(reference);
            if (messageLength == -1) {
                return 0;
            } else if (messageLength == 0) {
                aggregator.pingReceived(channel);
                return 6;
            } else {
                int headerBytesToRead = headerBytesToRead(reference);
                if (headerBytesToRead == 0) {
                    return 0;
                } else {
                    totalNetworkSize = messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE;

                    Header header = readHeader(messageLength, reference);
                    bytesConsumed += headerBytesToRead;
                    if (header.isCompressed()) {
                        decompressor = new TransportDecompressor(recycler);
                    }
                    aggregator.headerReceived(header);

                    if (isDone()) {
                        finishMessage(channel);
                    }
                    return bytesConsumed;
                }
            }
        } else {
            int bytesToConsume = Math.min(reference.length(), totalNetworkSize - bytesConsumed);
            bytesConsumed += bytesToConsume;
            ReleasableBytesReference retainedContent;
            if (isDone()) {
                retainedContent = reference.retainedSlice(0, bytesToConsume);
            } else {
                retainedContent = reference.retain();
            }
            if (decompressor != null) {
                decompress(retainedContent);
                ReleasableBytesReference decompressed;
                while ((decompressed = decompressor.pollDecompressedPage()) != null) {
                    forwardNonEmptyContent(channel, decompressed);
                }
            } else {
                forwardNonEmptyContent(channel, retainedContent);
            }
            if (isDone()) {
                finishMessage(channel);
            }

            return bytesToConsume;
        }
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(decompressor);
        decompressor = null;
        totalNetworkSize = -1;
        bytesConsumed = 0;
    }

    private void forwardNonEmptyContent(TcpChannel channel, ReleasableBytesReference content) {
        // Do not bother forwarding empty content
        if (content.length() == 0) {
            content.close();
        } else {
            aggregator.contentReceived(channel, content);
        }
    }

    private void finishMessage(TcpChannel channel) {
        decompressor = null;
        totalNetworkSize = -1;
        bytesConsumed = 0;
        aggregator.contentReceived(channel, END_CONTENT);
    }

    private void decompress(ReleasableBytesReference content) throws IOException {
        try (content) {
            int consumed = decompressor.decompress(content);
            assert consumed == content.length();
        }
    }

    private boolean isDone() {
        return bytesConsumed == totalNetworkSize;
    }

    private int headerBytesToRead(BytesReference reference) {
        if (reference.length() < TcpHeader.BYTES_REQUIRED_FOR_VERSION) {
            return 0;
        }

        Version remoteVersion = Version.fromId(reference.getInt(TcpHeader.VERSION_POSITION));
        int fixedHeaderSize = TcpHeader.headerSize(remoteVersion);
        if (fixedHeaderSize > reference.length()) {
            return 0;
        } else if (remoteVersion.before(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
            return fixedHeaderSize;
        } else {
            int variableHeaderSize = reference.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
            int totalHeaderSize = fixedHeaderSize + variableHeaderSize;
            if (totalHeaderSize > reference.length()) {
                return 0;
            } else {
                return totalHeaderSize;
            }
        }
    }

    private Header readHeader(int networkMessageSize, BytesReference bytesReference) throws IOException {
        try (StreamInput streamInput = bytesReference.streamInput()) {
            streamInput.skip(TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            long requestId = streamInput.readLong();
            byte status = streamInput.readByte();
            Version remoteVersion = Version.fromId(streamInput.readInt());
            Header header = new Header(networkMessageSize, requestId, status, remoteVersion);
            if (remoteVersion.onOrAfter(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
                // Skip since we already have ensured enough data available
                streamInput.readInt();
                header.finishParsingHeader(streamInput);
            }
            return header;
        }
    }

    private boolean isOnHeader() {
        return totalNetworkSize == -1;
    }
}
