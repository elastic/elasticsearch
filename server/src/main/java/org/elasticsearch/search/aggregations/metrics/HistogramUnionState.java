/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;
import org.elasticsearch.exponentialhistogram.TDigestToExponentialHistogramConverter;
import org.elasticsearch.tdigest.Centroid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A state for holding potentially mixed data of exponential histogram and T-Digests.
 * If the data is not mixed, the querying methods will just delegate to {@link ExponentialHistogramState} or {@link TDigestState} as appropriate.
 * Otherwise the T-Digest data will be converted to exponential histogram and will be merged with the existing exponential histogram data to serve queries.
 */
public class HistogramUnionState implements Releasable, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HistogramUnionState.class);

    private final double tdigestCompression;
    private final TDigestState.Type tdigestType;
    private CircuitBreaker breaker;

    // These are initialized lazily when needed
    private TDigestState tDigestState;
    private ExponentialHistogramState exponentialHistogramState;

    // this acts as a cache when mixed data is queried multiple times
    private ExponentialHistogramState combinedState;

    private HistogramUnionState(
        CircuitBreaker breaker,
        TDigestState.Type type,
        double tDigestCompression,
        @Nullable ExponentialHistogramState expHistoState
    ) {
        this.breaker = breaker;
        this.tdigestType = type;
        this.tdigestCompression = tDigestCompression;
        this.exponentialHistogramState = expHistoState;
    }

    private HistogramUnionState(CircuitBreaker breaker, TDigestState tdigestState, @Nullable ExponentialHistogramState expHistoState) {
        assert tdigestState != null;
        this.breaker = breaker;
        this.exponentialHistogramState = expHistoState;
        this.tDigestState = tdigestState;
        // These are not needed and not used as tDigestState is already initialized
        this.tdigestType = null;
        this.tdigestCompression = -1;
    }

    public static HistogramUnionState create(CircuitBreaker breaker, TDigestState.Type type, double tDigestCompression) {
        return createWithEmptyTDigest(breaker, type, tDigestCompression, null);
    }

    private static HistogramUnionState createWithEmptyTDigest(
        CircuitBreaker breaker,
        TDigestState.Type type,
        double tDigestCompression,
        @Nullable ExponentialHistogramState expHistoState
    ) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "histogram-union-state-create");
        try {
            return new HistogramUnionState(breaker, type, tDigestCompression, expHistoState);
        } catch (Exception e) {
            breaker.addWithoutBreaking(-SHALLOW_SIZE);
            throw e;
        }
    }

    private static HistogramUnionState createWithPopulatedTDigest(
        CircuitBreaker breaker,
        TDigestState populatedTDigest,
        @Nullable ExponentialHistogramState expHistoState
    ) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "histogram-union-state-create");
        try {
            return new HistogramUnionState(breaker, populatedTDigest, expHistoState);
        } catch (Exception e) {
            breaker.addWithoutBreaking(-SHALLOW_SIZE);
            throw e;
        }
    }

    public void add(HistogramUnionState other) {
        if (other.exponentialHistogramState != null) {
            getOrInitializeExponentialHistogramState().addWithoutUpscaling(other.exponentialHistogramState.histogram());
            invalidateCachedCombinedState();
        }
        if (other.tDigestState != null) {
            getOrInitializeTDigestState().add(other.tDigestState);
            invalidateCachedCombinedState();
        }
    }

    public void add(ExponentialHistogram histogram) {
        getOrInitializeExponentialHistogramState().add(histogram);
        invalidateCachedCombinedState();
    }

    public void add(TDigestState tdigest) {
        getOrInitializeTDigestState().add(tdigest);
        invalidateCachedCombinedState();
    }

    public final void compress() {
        if (tDigestState != null) {
            invalidateCachedCombinedState();
            tDigestState.compress();
        }
    }

    public final long size() {
        long size = 0;
        if (exponentialHistogramState != null) {
            size += exponentialHistogramState.size();
        }
        if (tDigestState != null) {
            size += tDigestState.size();
        }
        return size;
    }

    public final double cdf(double x) {
        if (tDigestState != null && exponentialHistogramState != null) {
            return getCombinedState().cdf(x);
        } else if (tDigestState != null) {
            return tDigestState.cdf(x);
        } else if (exponentialHistogramState != null) {
            return exponentialHistogramState.cdf(x);
        } else {
            return Double.NaN;
        }
    }

    public final double quantile(double q) {
        if (tDigestState != null && exponentialHistogramState != null) {
            return getCombinedState().quantile(q);
        } else if (tDigestState != null) {
            return tDigestState.quantile(q);
        } else if (exponentialHistogramState != null) {
            return exponentialHistogramState.quantile(q);
        } else {
            return Double.NaN;
        }
    }

    public final Collection<Centroid> centroids() {
        if (tDigestState != null && exponentialHistogramState != null) {
            return getCombinedState().centroids();
        } else if (tDigestState != null) {
            return tDigestState.centroids();
        } else if (exponentialHistogramState != null) {
            return exponentialHistogramState.centroids();
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public long ramBytesUsed() {
        long result = SHALLOW_SIZE;
        if (exponentialHistogramState != null) {
            result += exponentialHistogramState.ramBytesUsed();
        }
        if (tDigestState != null) {
            result += tDigestState.ramBytesUsed();
        }
        if (combinedState != null) {
            result += combinedState.ramBytesUsed();
        }
        return result;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(exponentialHistogramState != null);
        if (exponentialHistogramState != null) {
            exponentialHistogramState.write(out);
        }
        out.writeBoolean(tDigestState != null);
        if (tDigestState != null) {
            TDigestState.write(tDigestState, out);
        } else {
            out.writeString(tdigestType.toString());
            out.writeDouble(tdigestCompression);
        }
    }

    public static HistogramUnionState read(CircuitBreaker breaker, StreamInput in) throws IOException {
        ExponentialHistogramState exponentialHistogramState = null;
        TDigestState tdigestState = null;
        boolean success = false;
        try {

            if (in.readBoolean()) {
                exponentialHistogramState = ExponentialHistogramState.read(breaker, in);
            }
            HistogramUnionState result;
            if (in.readBoolean()) {
                tdigestState = TDigestState.read(breaker, in);
                result = createWithPopulatedTDigest(breaker, tdigestState, exponentialHistogramState);
            } else {
                TDigestState.Type type = TDigestState.Type.valueOf(in.readString());
                double compression = in.readDouble();
                result = createWithEmptyTDigest(breaker, type, compression, exponentialHistogramState);
            }
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.close(exponentialHistogramState, tdigestState);
            }
        }
    }

    /**
     * Writes this state so that it can be deserialized via {@link TDigestState#read(StreamInput)}.
     */
    public void writeAsPureTDigestTo(StreamOutput out) throws IOException {
        if (exponentialHistogramState != null) {
            throw new IllegalStateException("This state contains exponential histogram data and cannot be serialized as pure T-Digest");
        }
        TDigestState.write(getOrInitializeTDigestState(), out);
    }

    /**
     * Reads a {@link HistogramUnionState} serialized via {@link #writeAsPureTDigestTo(StreamOutput)}.
     * This means it is also capable of serializing data which was written via {@link TDigestState#write(TDigestState, StreamOutput)}.
     */
    public static HistogramUnionState readAsPureTDigest(CircuitBreaker breaker, StreamInput in) throws IOException {
        TDigestState tdigestState = null;
        try {
            tdigestState = TDigestState.read(breaker, in);
            HistogramUnionState result = createWithPopulatedTDigest(breaker, tdigestState, null);
            tdigestState = null;
            return result;
        } finally {
            Releasables.close(tdigestState);
        }
    }

    private TDigestState getOrInitializeTDigestState() {
        if (tDigestState == null) {
            tDigestState = TDigestState.createOfType(breaker, tdigestType, tdigestCompression);
        }
        return tDigestState;
    }

    private ExponentialHistogramState getOrInitializeExponentialHistogramState() {
        if (exponentialHistogramState == null) {
            exponentialHistogramState = ExponentialHistogramState.create(breaker);
        }
        return exponentialHistogramState;
    }

    private ExponentialHistogramState getCombinedState() {
        if (exponentialHistogramState == null) {
            throw new IllegalStateException("This state does not contain exponential histogram data");
        }
        if (tDigestState == null) {
            throw new IllegalStateException("This state does not contain exponential histogram data");
        }
        if (combinedState == null) {
            combinedState = ExponentialHistogramState.create(breaker);
            combinedState.addWithoutUpscaling(exponentialHistogramState.histogram());
            try (
                ReleasableExponentialHistogram converted = TDigestToExponentialHistogramConverter.convert(
                    new CentroidIterator(tDigestState.centroids()),
                    new ExponentialHistogramState.ElasticCircuitBreakerWrapper(breaker)
                )
            ) {
                combinedState.add(converted);
            }
        }
        return combinedState;
    }

    private void invalidateCachedCombinedState() {
        if (combinedState != null) {
            Releasables.close(combinedState);
            combinedState = null;
        }
    }

    @Override
    public void close() {
        Releasables.close(exponentialHistogramState, tDigestState, combinedState);
        breaker.addWithoutBreaking(-SHALLOW_SIZE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HistogramUnionState that = (HistogramUnionState) o;

        // ensure both tDigestStates are either null or equal
        if (tDigestState == null && that.tDigestState == null) {
            if (Double.compare(that.tdigestCompression, tdigestCompression) != 0) return false;
            if (tdigestType != that.tdigestType) return false;
        } else if (tDigestState != null && that.tDigestState != null) {
            if (tDigestState.equals(that.tDigestState) == false) return false;
        } else {
            // one is null and the other is not
            return false;
        }
        return Objects.equals(exponentialHistogramState, that.exponentialHistogramState);
    }

    @Override
    public int hashCode() {
        if (tDigestState == null) {
            return Objects.hash(tdigestType, tdigestCompression, exponentialHistogramState);
        } else {
            return Objects.hash(tDigestState, exponentialHistogramState);
        }
    }

    private static class CentroidIterator implements TDigestToExponentialHistogramConverter.CentroidIterator {
        private final List<Centroid> centroids;
        int pos = 0;
        int direction = 1;

        CentroidIterator(Collection<Centroid> centroids) {
            if (centroids instanceof List) {
                this.centroids = (List<Centroid>) centroids;
            } else {
                this.centroids = new ArrayList<>(centroids);
            }
        }

        @Override
        public boolean hasNext() {
            return pos >= 0 && pos < centroids.size();
        }

        @Override
        public double value() {
            return centroids.get(pos).mean();
        }

        @Override
        public long count() {
            return centroids.get(pos).count();
        }

        @Override
        public void advance() {
            pos += direction;
        }

        @Override
        public TDigestToExponentialHistogramConverter.CentroidIterator reversedCopy() {
            var result = new CentroidIterator(centroids);
            result.pos = this.pos - direction;
            result.direction = -this.direction;
            return result;
        }
    }
}
