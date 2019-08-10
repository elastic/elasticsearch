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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class InboundDecoder {

    private final InboundAggregator aggregator;
    private TransportDecompressor decompressor;
    private int networkMessageSize = -1;

    public InboundDecoder(InboundAggregator aggregator) {
        this.aggregator = aggregator;
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
                networkMessageSize = expectedLength;
                if (releasable.getReference().length() < TcpHeader.HEADER_SIZE) {
                    releasable.close();
                    return 6;
                } else {

                }
            }
        }

        return releasable.getReference().length();
    }

    private Header parseHeader(BytesReference bytesReference) throws IOException {
        try (StreamInput streamInput = bytesReference.streamInput()) {
            long requestId = streamInput.readLong();
            byte status = streamInput.readByte();
            Version remoteVersion = Version.fromId(streamInput.readInt());
            return new Header(requestId, status, remoteVersion);
        }
    }

    private boolean isOnHeader() {
        return networkMessageSize == -1;
    }

    public static class Header {

        private final Version version;
        private final long requestId;
        private final byte status;

        private Header(long requestId, byte status, Version version) {
            this.version = version;
            this.requestId = requestId;
            this.status = status;
        }
    }
}
