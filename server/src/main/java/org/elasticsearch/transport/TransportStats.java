/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

public class TransportStats implements Writeable, ToXContentFragment {

    private final long serverOpen;
    private final long totalOutboundConnections;
    private final long rxCount;
    private final long rxSize;
    private final long txCount;
    private final long txSize;
    private final long[] handlingTimeBucketFrequencies;

    public TransportStats(
        long serverOpen,
        long totalOutboundConnections,
        long rxCount,
        long rxSize,
        long txCount,
        long txSize,
        long[] handlingTimeBucketFrequencies
    ) {
        this.serverOpen = serverOpen;
        this.totalOutboundConnections = totalOutboundConnections;
        this.rxCount = rxCount;
        this.rxSize = rxSize;
        this.txCount = txCount;
        this.txSize = txSize;
        this.handlingTimeBucketFrequencies = handlingTimeBucketFrequencies;
        assert assertHistogramConsistent();
    }

    public TransportStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        totalOutboundConnections = in.readVLong();
        rxCount = in.readVLong();
        rxSize = in.readVLong();
        txCount = in.readVLong();
        txSize = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_8_1_0) && in.readBoolean()) {
            handlingTimeBucketFrequencies = new long[HandlingTimeTracker.BUCKET_COUNT];
            for (int i = 0; i < handlingTimeBucketFrequencies.length; i++) {
                handlingTimeBucketFrequencies[i] = in.readVLong();
            }
        } else {
            handlingTimeBucketFrequencies = new long[0];
        }
        assert assertHistogramConsistent();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverOpen);
        out.writeVLong(totalOutboundConnections);
        out.writeVLong(rxCount);
        out.writeVLong(rxSize);
        out.writeVLong(txCount);
        out.writeVLong(txSize);
        if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
            out.writeBoolean(handlingTimeBucketFrequencies.length > 0);
            for (long handlingTimeBucketFrequency : handlingTimeBucketFrequencies) {
                out.writeVLong(handlingTimeBucketFrequency);
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

    public long[] getHandlingTimeBucketFrequencies() {
        return Arrays.copyOf(handlingTimeBucketFrequencies, handlingTimeBucketFrequencies.length);
    }

    private boolean assertHistogramConsistent() {
        if (handlingTimeBucketFrequencies.length == 0) {
            // Stats came from before v8.1
            assert Version.CURRENT.major == Version.V_8_0_0.major;
        } else {
            assert handlingTimeBucketFrequencies.length == HandlingTimeTracker.BUCKET_COUNT;
        }
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSPORT);
        builder.field(Fields.SERVER_OPEN, serverOpen);
        builder.field(Fields.TOTAL_OUTBOUND_CONNECTIONS, totalOutboundConnections);
        builder.field(Fields.RX_COUNT, rxCount);
        builder.humanReadableField(Fields.RX_SIZE_IN_BYTES, Fields.RX_SIZE, new ByteSizeValue(rxSize));
        builder.field(Fields.TX_COUNT, txCount);
        builder.humanReadableField(Fields.TX_SIZE_IN_BYTES, Fields.TX_SIZE, new ByteSizeValue(txSize));
        if (handlingTimeBucketFrequencies.length > 0) {
            final int[] handlingTimeBucketBounds = HandlingTimeTracker.getBucketUpperBounds();
            assert handlingTimeBucketFrequencies.length == handlingTimeBucketBounds.length + 1;
            builder.startArray(Fields.HANDLING_TIME_HISTOGRAM);
            for (int i = 0; i < handlingTimeBucketFrequencies.length; i++) {
                builder.startObject();
                if (i > 0 && i <= handlingTimeBucketBounds.length) {
                    builder.field("ge_millis", handlingTimeBucketBounds[i - 1]);
                }
                if (i < handlingTimeBucketBounds.length) {
                    builder.field("lt_millis", handlingTimeBucketBounds[i]);
                }
                builder.field("count", handlingTimeBucketFrequencies[i]);
                builder.endObject();
            }
            builder.endArray();
        } else {
            // Stats came from before v8.1
            assert Version.CURRENT.major == Version.V_8_0_0.major;
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String TRANSPORT = "transport";
        static final String SERVER_OPEN = "server_open";
        static final String TOTAL_OUTBOUND_CONNECTIONS = "total_outbound_connections";
        static final String RX_COUNT = "rx_count";
        static final String RX_SIZE = "rx_size";
        static final String RX_SIZE_IN_BYTES = "rx_size_in_bytes";
        static final String TX_COUNT = "tx_count";
        static final String TX_SIZE = "tx_size";
        static final String TX_SIZE_IN_BYTES = "tx_size_in_bytes";
        static final String HANDLING_TIME_HISTOGRAM = "handling_time_histogram";
    }
}
