/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.tdigest.TDigestReadView;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/**
 * This is a {@link TDigestReadView} annotated with some extra information: The sum, min and max of all observations.
 * In addition, it stores the size as a dedicated field for faster access.
 * The TDigest is represented a list of centroids and their counts, encoded in a byte array.
 * This class does not own the underlying memory used to store the digest, it is merely a pointer/accessor for e.g.
 * a single value in a {@link TDigestBlock} or in {@link BreakingTDigestHolder}.
 * <br>
 * This class supports serialization, but this is only intended for use in ES|QL Literals, as it uses untracked memory on deserialization.
 */
public class TDigestHolder implements GenericNamedWriteable, TDigestReadView {

    /**
     * This size of a single TDigestHolder instance in bytes, excluding the underlying encoded digest bytes array.
     * The encoded digest bytes are not owned by this class, so they are not included in the accounting.
     */
    static final long RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(TDigestHolder.class) + RamUsageEstimator.shallowSizeOfInstance(
        BytesRef.class
    ) + EncodedTDigest.RAM_BYTES;

    private static final TDigestHolder EMPTY = new TDigestHolder() {
        @Override
        public void reset(BytesRef encodedDigest, double min, double max, double sum, long valueCount) {
            throw new UnsupportedOperationException("This instance is immutable");
        }
    };

    private static final TransportVersion ESQL_SERIALIZEABLE_TDIGEST = TransportVersion.fromName("esql_serializeable_tdigest");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        GenericNamedWriteable.class,
        "TDigestHolder",
        TDigestHolder::new
    );

    private final EncodedTDigest encodedDigest = new EncodedTDigest();
    private final BytesRef scratchBytesRef = new BytesRef();

    private double min = Double.NaN;
    private double max = Double.NaN;
    private double sum = Double.NaN;
    private long valueCount = 0L;

    public TDigestHolder() {}

    BytesRef scratchBytesRef() {
        return scratchBytesRef;
    }

    // Note that this constructor allocates the bytes without memory accounting
    public TDigestHolder(StreamInput in) throws IOException {
        this.encodedDigest.reset(in.readBytesRef());
        this.min = in.readDouble();
        this.max = in.readDouble();
        this.sum = in.readDouble();
        this.valueCount = in.readVLong();
    }

    public void reset(BytesRef encodedDigest, double min, double max, double sum, long valueCount) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.valueCount = valueCount;
        this.encodedDigest.reset(encodedDigest);
    }

    public static TDigestHolder empty() {
        return EMPTY;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesRef(encodedDigest.encodedDigest());
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeDouble(sum);
        out.writeVLong(valueCount);
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof TDigestHolder that)) {
            return Double.compare(min, that.min) == 0
                && Double.compare(max, that.max) == 0
                && Double.compare(sum, that.sum) == 0
                && valueCount == that.valueCount
                && Objects.equals(encodedDigest.encodedDigest(), that.encodedDigest.encodedDigest());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, sum, valueCount, encodedDigest.encodedDigest());
    }

    public BytesRef getEncodedDigest() {
        return encodedDigest.encodedDigest();
    }

    @Override
    public double getMax() {
        return max;
    }

    @Override
    public double getMin() {
        return min;
    }

    @Override
    public long size() {
        return valueCount;
    }

    @Override
    public Collection<Centroid> centroids() {
        return encodedDigest.centroids();
    }

    @Override
    public int centroidCount() {
        return encodedDigest.centroidCount();
    }

    public double getSum() {
        return sum;
    }

    @Override
    public String toString() {
        // TODO: this is largely duplicated from TDigestFieldMapepr's synthetic source support, and we should refactor all of that.
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();

            if (Double.isNaN(this.getMin()) == false) {
                builder.field("min", this.getMin());
            }
            if (Double.isNaN(this.getMax()) == false) {
                builder.field("max", this.getMax());
            }
            if (Double.isNaN(this.getSum()) == false) {
                builder.field("sum", this.getSum());
            }

            // TODO: reuse the constans from the field type

            builder.startArray("centroids");

            EncodedTDigest.CentroidIterator iterator = encodedDigest.centroidIterator();
            while (iterator.next()) {
                builder.value(iterator.currentMean());
            }
            builder.endArray();

            builder.startArray("counts");
            iterator = encodedDigest.centroidIterator();
            while (iterator.next()) {
                builder.value(iterator.currentCount());
            }
            builder.endArray();
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new IllegalStateException("error rendering TDigest", e);
        }
    }

    @Override
    public String getWriteableName() {
        return "TDigestHolder";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ESQL_SERIALIZEABLE_TDIGEST;
    }
}
