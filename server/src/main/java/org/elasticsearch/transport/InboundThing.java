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
import org.elasticsearch.common.bytes.ReleasableBytesReference;

import java.io.IOException;
import java.util.ArrayList;

public class InboundThing {

    private final TcpChannel tcpChannel;
    private final TcpTransport tcpTransport;
    private int messageLength = -1;
    private MessageHeader currentHeader;
    private final ArrayList<ReleasableBytesReference> content = new ArrayList<>();

    public InboundThing(TcpChannel tcpChannel, TcpTransport tcpTransport) {
        this.tcpChannel = tcpChannel;
        this.tcpTransport = tcpTransport;
    }

    public int handle(ReleasableBytesReference releasable) throws IOException {
        if (isOnHeader()) {
            int expectedLength = TcpTransport.readMessageLength(releasable.getReference());
            if (expectedLength == -1) {
                releasable.close();
                return 0;
            } else if (expectedLength == 0) {
                tcpTransport.inboundMessage(tcpChannel, releasable.getReference().slice(0, 6));
                releasable.close();
                return 6;
            } else {
                if (releasable.getReference().length() < TcpHeader.HEADER_SIZE) {
                    releasable.close();
                    return 0;
                } else {
                    messageLength = expectedLength;
                }
            }
        }

        return releasable.getReference().length();
    }

    private boolean isOnHeader() {
        return messageLength == -1;
    }

    private static class MessageHeader {

        private final Version version;
        private final long requestId;

        private MessageHeader(Version version, long requestId) {
            this.version = version;
            this.requestId = requestId;
        }
    }
}
