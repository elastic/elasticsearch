/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.analytics.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.tdigest.TDigestReadView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Read-only digest backed by encoded centroid bytes.
 * <p>
 * The encoded format is a sequence of pairs:
 * <ul>
 *     <li>centroid count as VLong</li>
 *     <li>centroid mean as IEEE754 double (8 bytes, big-endian)</li>
 * </ul>
 */
public final class EncodedTDigest implements TDigestReadView {

    /**
     * The size of a single EncodedTDigest instance in bytes, excluding the underlying encoded digest bytes array.
     */
    public static final long RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(EncodedTDigest.class) + RamUsageEstimator
        .shallowSizeOfInstance(BytesRef.class);

    private final BytesRef encodedDigest = new BytesRef();
    private long cachedSize = -1L;
    private double cachedMax = Double.NaN;
    private int cachedCentroidCount = -1;

    public EncodedTDigest() {}

    public EncodedTDigest(BytesRef encodedDigest) {
        reset(encodedDigest);
    }

    /**
     * Replaces the underlying encoded digest bytes.
     * All decoding happens lazily when methods like {@link #size()} are called.
     * The provided {@code encodedDigest} is copied shallowly, so the caller is responsible for ensuring
     * that the bytes remain unchanged for the lifetime of this instance.
     *
     * @param encodedDigest The new encoded digest bytes. Must not be null, but may be empty.
     */
    public void reset(BytesRef encodedDigest) {
        this.encodedDigest.bytes = encodedDigest.bytes;
        this.encodedDigest.offset = encodedDigest.offset;
        this.encodedDigest.length = encodedDigest.length;
        resetCachedStats();
    }

    /**
     * Returns the currently configured encoded digest bytes.
     */
    public BytesRef encodedDigest() {
        return encodedDigest;
    }

    /**
     * Returns an allocation-free iterator over encoded centroids.
     */
    public CentroidIterator centroidIterator() {
        return new EncodedCentroidIterator(encodedDigest);
    }

    /**
     * Encodes the provided centroids into a {@link BytesRef}.
     */
    public static BytesRef encodeCentroids(Collection<? extends Centroid> centroids) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            encodeCentroids(centroids, out);
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode centroid", e);
        }
    }

    public static void encodeCentroids(Collection<? extends Centroid> centroids, StreamOutput out) throws IOException {
        encodeCentroidsFromIterator(new CentroidIterator() {
            private final Iterator<? extends Centroid> iterator = centroids.iterator();
            Centroid centroid;

            @Override
            public boolean next() {
                if (iterator.hasNext() == false) {
                    return false;
                }
                centroid = iterator.next();
                return true;
            }

            @Override
            public long currentCount() {
                assert centroid != null : "next() must be called and return true before accessing current centroid";
                return centroid.count();
            }

            @Override
            public double currentMean() {
                assert centroid != null : "next() must be called and return true before accessing current centroid";
                return centroid.mean();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

        }, out);
    }

    /**
     * Encodes centroids represented by independent means and counts lists.
     */
    public static BytesRef encodeCentroids(List<Double> means, List<Long> counts) {
        assert means.size() == counts.size() : "centroids and counts must have equal size";
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            encodeCentroidsFromIterator(new CentroidIterator() {
                private int index = -1;

                @Override
                public boolean next() {
                    index++;
                    return index < means.size();
                }

                @Override
                public long currentCount() {
                    assert index >= 0 : "next() must be called and return true before accessing current centroid";
                    return counts.get(index);
                }

                @Override
                public double currentMean() {
                    assert index >= 0 : "next() must be called and return true before accessing current centroid";
                    return means.get(index);
                }

                @Override
                public boolean hasNext() {
                    return index + 1 < means.size();
                }
            }, out);
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode centroid", e);
        }
    }

    @Override
    public long size() {
        ensureCachedStatsPopulated();
        return cachedSize;
    }

    @Override
    public int centroidCount() {
        ensureCachedStatsPopulated();
        return cachedCentroidCount;
    }

    @Override
    public double getMin() {
        CentroidIterator iterator = centroidIterator();
        if (iterator.next() == false) {
            return Double.NaN;
        }
        return iterator.currentMean();
    }

    @Override
    public double getMax() {
        ensureCachedStatsPopulated();
        return cachedMax;
    }

    @Override
    public Collection<Centroid> centroids() {
        if (encodedDigest.length == 0) {
            return List.of();
        }
        List<Centroid> decoded = new ArrayList<>();
        CentroidIterator it = centroidIterator();
        while (it.next()) {
            decoded.add(new Centroid(it.currentMean(), it.currentCount()));
        }
        return Collections.unmodifiableList(decoded);
    }

    /**
     * Encodes the given centroids into the given output. Also compute the sum of all counts and returns it.
     *
     * @param centroids the centroids to encode
     * @param out the output to encode into
     * @return the sum of all counts in the centroids
     */
    public static long encodeCentroidsFromIterator(CentroidIterator centroids, StreamOutput out) throws IOException {
        long totalCount = 0;
        while (centroids.next()) {
            long count = centroids.currentCount();
            if (count < 0) {
                throw new IllegalArgumentException("Centroid count cannot be negative: " + count);
            }
            totalCount += count;
            if (count > 0) {
                out.writeVLong(count);
                out.writeDouble(centroids.currentMean());
            }
        }
        return totalCount;
    }

    private void resetCachedStats() {
        this.cachedSize = -1L;
        this.cachedMax = Double.NaN;
        this.cachedCentroidCount = -1;
    }

    private void ensureCachedStatsPopulated() {
        if (cachedSize >= 0) {
            return;
        }
        long size = 0L;
        double max = Double.NaN;
        int centroidCount = 0;
        CentroidIterator it = centroidIterator();
        while (it.next()) {
            size += it.currentCount();
            max = it.currentMean();
            centroidCount++;
        }
        cachedSize = size;
        cachedMax = max;
        cachedCentroidCount = centroidCount;
    }

    /**
     * Iterator over centroids in increasing mean order.
     */
    public interface CentroidIterator {
        boolean next();

        long currentCount();

        double currentMean();

        /**
         * Returns {@code true} iff a call to {@link #next()} would return {@code false}.
         */
        boolean hasNext();
    }

    private static final class EncodedCentroidIterator implements CentroidIterator {
        private final ByteArrayStreamInput input = new ByteArrayStreamInput();

        private long count;
        private double mean;

        private EncodedCentroidIterator(BytesRef encodedDigest) {
            this.input.reset(encodedDigest.bytes, encodedDigest.offset, encodedDigest.length);
            this.count = -1;
        }

        @Override
        public boolean next() {
            if (hasNext()) {
                try {
                    count = input.readVLong();
                    mean = input.readDouble();
                    return true;
                } catch (IOException e) {
                    throw new IllegalStateException("Malformed TDigest bytes", e);
                }
            }
            return false;
        }

        @Override
        public long currentCount() {
            assert count != -1 : "next() must be called and return true before accessing current centroid";
            return count;
        }

        @Override
        public double currentMean() {
            assert count != -1 : "next() must be called and return true before accessing current centroid";
            return mean;
        }

        @Override
        public boolean hasNext() {
            return input.available() > 0;
        }
    }
}
