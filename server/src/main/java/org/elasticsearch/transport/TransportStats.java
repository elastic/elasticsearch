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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class TransportStats implements Writeable, ToXContentFragment {

    private final long serverOpen;
    private final long rxCount;
    private final long rxSize;
    private final long txCount;
    private final long txSize;
    private static final Version CONNECTION_STATS_INTRODUCED = Version.V_8_0_0;
    private final InboundConnectionsStats inboundConnectionsStats;

    public InboundConnectionsStats getInboundConnectionsStats() {
        return inboundConnectionsStats;
    }

    public static class InboundConnectionsStats implements Writeable, ToXContentFragment {
        private final long openedChannels;
        private final long closedChannels;
        private final long requestsReceivedCount;
        private final long requestsReceivedBytes;
        private final long responsesSentCount;
        private final long responseSentBytes;
        private final long keepAlivePingsReceivedCount;
        private final long keepAlivePingsReceivedBytes;
        private final long keepAlivePongsSentCount;
        private final long keepAlivePongsSentBytes;

        static final String INBOUND_CONNECTIONS = "inbound_connections";
        static final String OPENED_CHANNELS = "opened_channels";
        static final String REQUESTS_RECEIVED_COUNT = "req_rcv_count";
        static final String REQUESTS_RECEIVED_SIZE = "req_rcv_size";
        static final String REQUESTS_RECEIVED_SIZE_IN_BYTES = "req_rcv_size_in_bytes";
        static final String RESPONSES_SENT_COUNT = "resp_sent_count";
        static final String RESPONSES_SENT_SIZE = "resp_sent_size";
        static final String RESPONSES_SENT_SIZE_IN_BYTES = "resp_sent_size_in_bytes";
        static final String KEEP_ALIVE_PINGS_RECEIVED_COUNT = "keep_alive_pings_rcv_count";
        static final String KEEP_ALIVE_PINGS_RECEIVED_SIZE = "keep_alive_pings_rcv_size";
        static final String KEEP_ALIVE_PINGS_RECEIVED_SIZE_IN_BYTES = "keep_alive_pings_rcv_size_in_bytes";
        static final String KEEP_ALIVE_PONGS_SENT_COUNT = "keep_alive_pongs_sent_count";
        static final String KEEP_ALIVE_PONGS_SENT_SIZE = "keep_alive_pongs_sent_size";
        static final String KEEP_ALIVE_PONGS_SENT_SIZE_IN_BYTES = "keep_alive_pongs_sent_size_in_bytes";

        public InboundConnectionsStats(long openedChannels, long closedChannels, long requestsReceivedCount, long requestsReceivedBytes,
                                long responsesSentCount, long responseSentBytes, long keepAlivePingsReceivedCount,
                                long keepAlivePingsReceivedBytes, long keepAlivePongsSentCount, long keepAlivePongsSentBytes) {
            this.openedChannels = openedChannels;
            this.closedChannels = closedChannels;
            this.requestsReceivedCount = requestsReceivedCount;
            this.requestsReceivedBytes = requestsReceivedBytes;
            this.responsesSentCount = responsesSentCount;
            this.responseSentBytes = responseSentBytes;
            this.keepAlivePingsReceivedCount = keepAlivePingsReceivedCount;
            this.keepAlivePingsReceivedBytes = keepAlivePingsReceivedBytes;
            this.keepAlivePongsSentCount = keepAlivePongsSentCount;
            this.keepAlivePongsSentBytes = keepAlivePongsSentBytes;
        }

        public InboundConnectionsStats(StreamInput in) throws IOException {
            this.openedChannels = in.readVLong();
            this.closedChannels = in.readVLong();
            this.requestsReceivedCount = in.readVLong();
            this.requestsReceivedBytes = in.readVLong();
            this.responsesSentCount = in.readVLong();
            this.responseSentBytes = in.readVLong();
            this.keepAlivePingsReceivedCount = in.readVLong();
            this.keepAlivePingsReceivedBytes = in.readVLong();
            this.keepAlivePongsSentCount = in.readVLong();
            this.keepAlivePongsSentBytes = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(openedChannels);
            out.writeVLong(closedChannels);
            out.writeVLong(requestsReceivedCount);
            out.writeVLong(requestsReceivedBytes);
            out.writeVLong(responsesSentCount);
            out.writeVLong(responseSentBytes);
            out.writeVLong(keepAlivePingsReceivedCount);
            out.writeVLong(keepAlivePingsReceivedBytes);
            out.writeVLong(keepAlivePongsSentCount);
            out.writeVLong(keepAlivePongsSentBytes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(INBOUND_CONNECTIONS);
            builder.field(OPENED_CHANNELS, openedChannels);
            builder.field(REQUESTS_RECEIVED_COUNT, requestsReceivedCount);
            builder.humanReadableField(REQUESTS_RECEIVED_SIZE_IN_BYTES, REQUESTS_RECEIVED_SIZE, new ByteSizeValue(requestsReceivedBytes));
            builder.field(RESPONSES_SENT_COUNT, responsesSentCount);
            builder.humanReadableField(RESPONSES_SENT_SIZE_IN_BYTES, RESPONSES_SENT_SIZE, new ByteSizeValue(responseSentBytes));
            builder.field(KEEP_ALIVE_PINGS_RECEIVED_COUNT, keepAlivePingsReceivedCount);
            builder.humanReadableField(KEEP_ALIVE_PINGS_RECEIVED_SIZE_IN_BYTES, KEEP_ALIVE_PINGS_RECEIVED_SIZE,
                    new ByteSizeValue(keepAlivePingsReceivedBytes));
            builder.field(KEEP_ALIVE_PONGS_SENT_COUNT, keepAlivePongsSentCount);
            builder.humanReadableField(KEEP_ALIVE_PONGS_SENT_SIZE_IN_BYTES, KEEP_ALIVE_PONGS_SENT_SIZE,
                    new ByteSizeValue(keepAlivePongsSentBytes));
            builder.endObject();
            return builder;
        }

        public long getOpenedChannels() {
            return openedChannels;
        }

        public long getClosedChannels() {
            return closedChannels;
        }

        public long getRequestsReceivedCount() {
            return requestsReceivedCount;
        }

        public long getRequestsReceivedBytes() {
            return requestsReceivedBytes;
        }

        public long getResponsesSentCount() {
            return responsesSentCount;
        }

        public long getResponseSentBytes() {
            return responseSentBytes;
        }

        public long getKeepAlivePingsReceivedCount() {
            return keepAlivePingsReceivedCount;
        }

        public long getKeepAlivePingsReceivedBytes() {
            return keepAlivePingsReceivedBytes;
        }

        public long getKeepAlivePongsSentCount() {
            return keepAlivePongsSentCount;
        }

        public long getKeepAlivePongsSentBytes() {
            return keepAlivePongsSentBytes;
        }
    }

    public TransportStats(long serverOpen, long rxCount, long rxSize, long txCount, long txSize,
                          InboundConnectionsStats inboundConnectionsStats) {
        this.serverOpen = serverOpen;
        this.rxCount = rxCount;
        this.rxSize = rxSize;
        this.txCount = txCount;
        this.txSize = txSize;
        this.inboundConnectionsStats = inboundConnectionsStats;
    }

    public TransportStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        rxCount = in.readVLong();
        rxSize = in.readVLong();
        txCount = in.readVLong();
        txSize = in.readVLong();
        if (in.getVersion().onOrAfter(CONNECTION_STATS_INTRODUCED)) {
            if (in.readBoolean()) {
                inboundConnectionsStats = new InboundConnectionsStats(in);
            } else {
                inboundConnectionsStats = null;
            }
        } else {
            inboundConnectionsStats = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverOpen);
        out.writeVLong(rxCount);
        out.writeVLong(rxSize);
        out.writeVLong(txCount);
        out.writeVLong(txSize);
        if (out.getVersion().onOrAfter(CONNECTION_STATS_INTRODUCED)) {
            if (inboundConnectionsStats != null) {
                out.writeBoolean(true);
                inboundConnectionsStats.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    public long serverOpen() {
        return this.serverOpen;
    }

    public long getServerOpen() {
        return serverOpen();
    }

    public long rxCount() {
        return rxCount;
    }

    public long getRxCount() {
        return rxCount();
    }

    public ByteSizeValue rxSize() {
        return new ByteSizeValue(rxSize);
    }

    public ByteSizeValue getRxSize() {
        return rxSize();
    }

    public long txCount() {
        return txCount;
    }

    public long getTxCount() {
        return txCount();
    }

    public ByteSizeValue txSize() {
        return new ByteSizeValue(txSize);
    }

    public ByteSizeValue getTxSize() {
        return txSize();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSPORT);
        builder.field(Fields.SERVER_OPEN, serverOpen);
        builder.field(Fields.RX_COUNT, rxCount);
        builder.humanReadableField(Fields.RX_SIZE_IN_BYTES, Fields.RX_SIZE, new ByteSizeValue(rxSize));
        builder.field(Fields.TX_COUNT, txCount);
        builder.humanReadableField(Fields.TX_SIZE_IN_BYTES, Fields.TX_SIZE, new ByteSizeValue(txSize));
        if (inboundConnectionsStats != null) {
            inboundConnectionsStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String TRANSPORT = "transport";
        static final String SERVER_OPEN = "server_open";
        static final String RX_COUNT = "rx_count";
        static final String RX_SIZE = "rx_size";
        static final String RX_SIZE_IN_BYTES = "rx_size_in_bytes";
        static final String TX_COUNT = "tx_count";
        static final String TX_SIZE = "tx_size";
        static final String TX_SIZE_IN_BYTES = "tx_size_in_bytes";
    }
}
