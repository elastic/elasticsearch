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
    private final long[] inboundHandlingTimeBucketFrequencies;
    private final long[] outboundHandlingTimeBucketFrequencies;

    public TransportStats(
        long serverOpen,
        long totalOutboundConnections,
        long rxCount,
        long rxSize,
        long txCount,
        long txSize,
        long[] inboundHandlingTimeBucketFrequencies,
        long[] outboundHandlingTimeBucketFrequencies
    ) {
        this.serverOpen = serverOpen;
        this.totalOutboundConnections = totalOutboundConnections;
        this.rxCount = rxCount;
        this.rxSize = rxSize;
        this.txCount = txCount;
        this.txSize = txSize;
        this.inboundHandlingTimeBucketFrequencies = inboundHandlingTimeBucketFrequencies;
        this.outboundHandlingTimeBucketFrequencies = outboundHandlingTimeBucketFrequencies;
        assert assertHistogramsConsistent();
    }

    public TransportStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        totalOutboundConnections = in.readVLong();
        rxCount = in.readVLong();
        rxSize = in.readVLong();
        txCount = in.readVLong();
        txSize = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_8_1_0) && in.readBoolean()) {
            inboundHandlingTimeBucketFrequencies = new long[HandlingTimeTracker.BUCKET_COUNT];
            for (int i = 0; i < inboundHandlingTimeBucketFrequencies.length; i++) {
                inboundHandlingTimeBucketFrequencies[i] = in.readVLong();
            }
            outboundHandlingTimeBucketFrequencies = new long[HandlingTimeTracker.BUCKET_COUNT];
            for (int i = 0; i < inboundHandlingTimeBucketFrequencies.length; i++) {
                outboundHandlingTimeBucketFrequencies[i] = in.readVLong();
            }
        } else {
            inboundHandlingTimeBucketFrequencies = new long[0];
            outboundHandlingTimeBucketFrequencies = new long[0];
        }
        assert assertHistogramsConsistent();
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
            assert (inboundHandlingTimeBucketFrequencies.length > 0) == (outboundHandlingTimeBucketFrequencies.length > 0);
            out.writeBoolean(inboundHandlingTimeBucketFrequencies.length > 0);
            for (long handlingTimeBucketFrequency : inboundHandlingTimeBucketFrequencies) {
                out.writeVLong(handlingTimeBucketFrequency);
            }
            for (long handlingTimeBucketFrequency : outboundHandlingTimeBucketFrequencies) {
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

    public long[] getInboundHandlingTimeBucketFrequencies() {
        return Arrays.copyOf(inboundHandlingTimeBucketFrequencies, inboundHandlingTimeBucketFrequencies.length);
    }

    public long[] getOutboundHandlingTimeBucketFrequencies() {
        return Arrays.copyOf(outboundHandlingTimeBucketFrequencies, outboundHandlingTimeBucketFrequencies.length);
    }

    private boolean assertHistogramsConsistent() {
        assert inboundHandlingTimeBucketFrequencies.length == outboundHandlingTimeBucketFrequencies.length;
        if (inboundHandlingTimeBucketFrequencies.length == 0) {
            // Stats came from before v8.1
            assert Version.CURRENT.major == Version.V_8_0_0.major;
        } else {
            assert inboundHandlingTimeBucketFrequencies.length == HandlingTimeTracker.BUCKET_COUNT;
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
        if (inboundHandlingTimeBucketFrequencies.length > 0) {
            histogramToXContent(builder, inboundHandlingTimeBucketFrequencies, Fields.INBOUND_HANDLING_TIME_HISTOGRAM);
            histogramToXContent(builder, outboundHandlingTimeBucketFrequencies, Fields.OUTBOUND_HANDLING_TIME_HISTOGRAM);
        } else {
            // Stats came from before v8.1
            assert Version.CURRENT.major == Version.V_8_0_0.major;
        }
        builder.endObject();
        return builder;
    }

    private static void histogramToXContent(XContentBuilder builder, long[] bucketFrequencies, String fieldName) throws IOException {
        final int[] bucketBounds = HandlingTimeTracker.getBucketUpperBounds();
        assert bucketFrequencies.length == bucketBounds.length + 1;
        builder.startArray(fieldName);
        for (int i = 0; i < bucketFrequencies.length; i++) {
            builder.startObject();
            if (i > 0 && i <= bucketBounds.length) {
                builder.field("ge_millis", bucketBounds[i - 1]);
            }
            if (i < bucketBounds.length) {
                builder.field("lt_millis", bucketBounds[i]);
            }
            builder.field("count", bucketFrequencies[i]);
            builder.endObject();
        }
        builder.endArray();
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
        static final String INBOUND_HANDLING_TIME_HISTOGRAM = "inbound_handling_time_histogram";
        static final String OUTBOUND_HANDLING_TIME_HISTOGRAM = "outbound_handling_time_histogram";
    }
}
