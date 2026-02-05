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
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
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
 * If the data is not mixed, the querying methods will just delegate to
 * {@link ExponentialHistogramState} or {@link TDigestState} as appropriate.
 * Otherwise the T-Digest data will be converted to exponential histogram and will be merged
 * with the existing exponential histogram data to serve queries.
 * This conversion will happen lazily: All T-Digests will be merged with each other and all exponential histograms
 * will be merged with each other first, to preserve accuracy as much as possible.
 * Only when a query is made that requires both data types, the conversion will happen.
 */
public class HistogramUnionState implements Releasable, Accountable {

    public static final HistogramUnionState EMPTY = new HistogramUnionState(
        new NoopCircuitBreaker("empty-state-cb"),
        new EmptyTDigestState(),
        null
    ) {
        @Override
        public void add(HistogramUnionState other) {
            throw new UnsupportedOperationException("Immutable Empty HistogramUnionState");
        }

        @Override
        public void add(ExponentialHistogram histogram) {
            throw new UnsupportedOperationException("Immutable Empty HistogramUnionState");
        }

        @Override
        public void add(double value) {
            throw new UnsupportedOperationException("Immutable Empty HistogramUnionState");
        }

        @Override
        public void add(double value, long count) {
            throw new UnsupportedOperationException("Immutable Empty HistogramUnionState");
        }

        @Override
        public void add(TDigestState tdigest) {
            throw new UnsupportedOperationException("Immutable Empty HistogramUnionState");
        }
    };

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HistogramUnionState.class);

    public static final CircuitBreaker NOOP_BREAKER = new NoopCircuitBreaker("histogram-union-state-noop-breaker");

    // Must be non-null if tDigestState is null
    private final TDigestInitParams tdigestInitParams;
    private CircuitBreaker breaker;

    // These are initialized lazily when needed
    private TDigestState tDigestState;
    private ExponentialHistogramState exponentialHistogramState;

    // Acts as a cache when mixed data is queried multiple times
    private ExponentialHistogramState combinedState;

    private HistogramUnionState(
        CircuitBreaker breaker,
        TDigestInitParams tdigestInitParams,
        @Nullable ExponentialHistogramState expHistoState
    ) {
        assert tdigestInitParams != null;
        this.breaker = breaker;
        this.tdigestInitParams = tdigestInitParams;
        this.exponentialHistogramState = expHistoState;
    }

    private HistogramUnionState(CircuitBreaker breaker, TDigestState tdigestState, @Nullable ExponentialHistogramState expHistoState) {
        assert tdigestState != null;
        this.breaker = breaker;
        this.exponentialHistogramState = expHistoState;
        this.tDigestState = tdigestState;
        // Not needed and not used as tDigestState is already initialized
        this.tdigestInitParams = null;
    }

    public static HistogramUnionState create(CircuitBreaker breaker, double tDigestCompression) {
        TDigestInitParams params = new TDigestInitParams(null, null, tDigestCompression);
        return createWithEmptyTDigest(breaker, params, null);
    }

    public static HistogramUnionState create(CircuitBreaker breaker, TDigestState.Type type, double tDigestCompression) {
        TDigestInitParams params = new TDigestInitParams(type, null, tDigestCompression);
        return createWithEmptyTDigest(breaker, params, null);
    }

    public static HistogramUnionState create(CircuitBreaker breaker, TDigestExecutionHint executionHint, double tDigestCompression) {
        TDigestInitParams params = new TDigestInitParams(null, executionHint, tDigestCompression);
        return createWithEmptyTDigest(breaker, params, null);
    }

    public static HistogramUnionState createUsingParamsFrom(HistogramUnionState otherState) {
        if (otherState.tDigestState != null) {
            return wrap(otherState.breaker, TDigestState.createUsingParamsFrom(otherState.tDigestState));
        } else {
            return createWithEmptyTDigest(otherState.breaker, otherState.tdigestInitParams, null);
        }
    }

    public static HistogramUnionState wrap(CircuitBreaker breaker, TDigestState tdigest) {
        return createWithPopulatedTDigest(breaker, tdigest, null);
    }

    private static HistogramUnionState createWithEmptyTDigest(
        CircuitBreaker breaker,
        TDigestInitParams tdigestInitParams,
        @Nullable ExponentialHistogramState expHistoState
    ) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "histogram-union-state-create");
        try {
            return new HistogramUnionState(breaker, tdigestInitParams, expHistoState);
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
            add(other.tDigestState);
        }
    }

    public void add(ExponentialHistogram histogram) {
        getOrInitializeExponentialHistogramState().add(histogram);
        invalidateCachedCombinedState();
    }

    public void add(double value) {
        getOrInitializeTDigestState().add(value);
        invalidateCachedCombinedState();
    }

    public void add(double value, long count) {
        getOrInitializeTDigestState().add(value, count);
        invalidateCachedCombinedState();
    }

    public void add(TDigestState tdigest) {
        getOrInitializeTDigestState().add(tdigest);
        invalidateCachedCombinedState();
    }

    public double compression() {
        if (tDigestState != null) {
            return tDigestState.compression();
        } else {
            return tdigestInitParams.compression();
        }
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

    public final double getMin() {
        double min = Double.POSITIVE_INFINITY;
        if (tDigestState != null) {
            min = Math.min(min, tDigestState.getMin());
        }
        if (exponentialHistogramState != null) {
            min = Math.min(min, exponentialHistogramState.getMin());
        }
        return min;
    }

    public final double getMax() {
        double max = Double.NEGATIVE_INFINITY;
        if (tDigestState != null) {
            max = Math.max(max, tDigestState.getMax());
        }
        if (exponentialHistogramState != null) {
            max = Math.max(max, exponentialHistogramState.getMax());
        }
        return max;
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
            tdigestInitParams.writeTo(out);
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
                TDigestInitParams initParams = TDigestInitParams.readFrom(in);
                result = createWithEmptyTDigest(breaker, initParams, exponentialHistogramState);
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
            tDigestState = tdigestInitParams.createTDigestState(breaker);
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
            if (tdigestInitParams.equals(that.tdigestInitParams) == false) return false;
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
            return Objects.hash(tdigestInitParams, exponentialHistogramState);
        } else {
            return Objects.hash(tDigestState, exponentialHistogramState);
        }
    }

    /**
     * This iterator is required as an input for the conversion algorithm from T-Digest to Exponential Histogram.
     * It needs access to the centroids in a list, so that it can be traversed both forwards and backwards.
     */
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

    private record TDigestInitParams(TDigestState.Type type, TDigestExecutionHint executionHint, double compression) {

        TDigestInitParams {
            assert type == null || executionHint == null : "Only one of type or executionHint can be non-null";
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(type == null ? null : type.toString());
            out.writeOptionalString(executionHint == null ? null : executionHint.toString());
            out.writeDouble(compression);
        }

        static TDigestInitParams readFrom(StreamInput in) throws IOException {
            String typeString = in.readOptionalString();
            String executionHintString = in.readOptionalString();
            return new TDigestInitParams(
                typeString == null ? null : TDigestState.Type.valueOf(typeString),
                executionHintString == null ? null : TDigestExecutionHint.parse(executionHintString),
                in.readDouble()
            );
        }

        TDigestState createTDigestState(CircuitBreaker breaker) {
            if (type != null) {
                return TDigestState.createOfType(breaker, type, compression);
            } else if (executionHint != null) {
                return TDigestState.create(breaker, compression, executionHint);
            } else {
                return TDigestState.create(breaker, compression);
            }

        }
    }
}
