/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.analytics.mapper.TDigestParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * This exists to hold the values from a {@link TDigestBlock}.  It is roughly parallel to
 * {@link org.elasticsearch.search.aggregations.metrics.TDigestState} in classic aggregations, which we are not using directly because
 * the serialization format is pretty bad for ESQL's use case (specifically, encoding the near-constant compression and merge strategy
 * data inline as opposed to in a dedicated column isn't great).
 *
 * This is writable to support ESQL literals of this type, even though those should not exist.  Literal support, and thus a writeable
 * object here, are required for ESQL testing.  See for example ShowExecSerializationTest.
 */
public class TDigestHolder implements GenericNamedWriteable {

    private static final TDigestHolder EMPTY;
    static {
        try {
            EMPTY = new TDigestHolder(encodeCentroidsAndCounts(List.of(), List.of()), Double.NaN, Double.NaN, Double.NaN, 0L);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final TransportVersion ESQL_SERIALIZEABLE_TDIGEST = TransportVersion.fromName("esql_serializeable_tdigest");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        GenericNamedWriteable.class,
        "TDigestHolder",
        TDigestHolder::new
    );

    private final double min;
    private final double max;
    private final double sum;
    private final long valueCount;
    private final BytesRef encodedDigest;

    // TODO - Deal with the empty array case better
    public TDigestHolder(BytesRef encodedDigest, double min, double max, double sum, long valueCount) {
        this.encodedDigest = encodedDigest;
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.valueCount = valueCount;
    }

    public TDigestHolder(TDigestState rawTDigest, double min, double max, double sum, long valueCount) {
        try {
            this.encodedDigest = encodeCentroidsAndCounts(rawTDigest);
        } catch (IOException e) {
            throw new IllegalStateException("Error encoding TDigest", e);
        }
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.valueCount = valueCount;
    }

    // TODO: Probably TDigestHolder and ParsedTDigest should be the same object
    public TDigestHolder(TDigestParser.ParsedTDigest parsed) throws IOException {
        this(parsed.centroids(), parsed.counts(), parsed.min(), parsed.max(), parsed.sum(), parsed.count());
    }

    public TDigestHolder(List<Double> centroids, List<Long> counts, double min, double max, double sum, long valueCount)
        throws IOException {
        this(encodeCentroidsAndCounts(centroids, counts), min, max, sum, valueCount);
    }

    public static TDigestHolder empty() {
        return EMPTY;
    }

    public TDigestHolder(StreamInput in) throws IOException {
        this.encodedDigest = in.readBytesRef();
        this.min = in.readDouble();
        this.max = in.readDouble();
        this.sum = in.readDouble();
        this.valueCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesRef(encodedDigest);
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
                && Objects.equals(encodedDigest, that.encodedDigest);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, sum, valueCount, encodedDigest);
    }

    private static BytesRef encodeCentroidsAndCounts(List<Double> centroids, List<Long> counts) throws IOException {
        // TODO: This is copied from the method of the same name in TDigestFieldMapper. It would be nice to find a way to reuse that code
        BytesStreamOutput streamOutput = new BytesStreamOutput();

        for (int i = 0; i < centroids.size(); i++) {
            long count = counts.get(i);
            assert count >= 0;
            // we do not add elements with count == 0
            if (count > 0) {
                streamOutput.writeVLong(count);
                streamOutput.writeDouble(centroids.get(i));
            }
        }

        BytesRef docValue = streamOutput.bytes().toBytesRef();
        return docValue;
    }

    private static BytesRef encodeCentroidsAndCounts(TDigestState rawTDigest) throws IOException {
        // TODO: This is copied from the method of the same name in TDigestFieldMapper. It would be nice to find a way to reuse that code
        BytesStreamOutput streamOutput = new BytesStreamOutput();

        for (Iterator<Centroid> it = rawTDigest.uniqueCentroids(); it.hasNext();) {
            Centroid centroid = it.next();
            if (centroid.count() > 0) {
                streamOutput.writeVLong(centroid.count());
                streamOutput.writeDouble(centroid.mean());
            }
        }

        BytesRef docValue = streamOutput.bytes().toBytesRef();
        return docValue;
    }

    public void addTo(TDigestState state) {
        try {
            // TODO: The decoding is copied from TDigestFieldMapper. It would be nice to find a way to reuse that code
            ByteArrayStreamInput values = new ByteArrayStreamInput();
            values.reset(encodedDigest.bytes, encodedDigest.offset, encodedDigest.length);
            while (values.available() > 0) {
                long count = values.readVLong();
                double centroid = values.readDouble();
                state.add(centroid, count);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Malformed TDigest bytes", e);
        }
    }

    public BytesRef getEncodedDigest() {
        return encodedDigest;
    }

    // TODO - compute these if they're not given? or do that at object creation time, maybe.
    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }

    public double getSum() {
        return sum;
    }

    public long getValueCount() {
        return valueCount;
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

            // TODO: Would be nice to wrap all of this in reusable objects and minimize allocations here
            ByteArrayStreamInput values = new ByteArrayStreamInput();
            values.reset(encodedDigest.bytes, encodedDigest.offset, encodedDigest.length);
            List<Double> centroids = new ArrayList<>();
            List<Long> counts = new ArrayList<>();
            while (values.available() > 0) {
                counts.add(values.readVLong());
                centroids.add(values.readDouble());
            }

            // TODO: reuse the constans from the field type
            builder.startArray("centroids");
            for (Double centroid : centroids) {
                builder.value(centroid.doubleValue());
            }
            builder.endArray();

            builder.startArray("counts");
            for (Long count : counts) {
                builder.value(count.longValue());
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
