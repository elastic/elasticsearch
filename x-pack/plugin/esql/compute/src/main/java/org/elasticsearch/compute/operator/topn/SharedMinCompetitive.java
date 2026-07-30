/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.SideChannel;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.compute.operator.topn.TopNOperator.BIG_NULL;
import static org.elasticsearch.compute.operator.topn.TopNOperator.SMALL_NULL;

/**
 * A thread safe, shared holder for the min competitive value from a
 * set of {@link TopNOperator}s.
 */
public class SharedMinCompetitive extends SideChannel {
    public record KeyConfig(ElementType elementType, TopNEncoder encoder, boolean asc, boolean nullsFirst) {}

    public static class Supplier extends SideChannel.Supplier<SharedMinCompetitive> {
        private final CircuitBreaker breaker;
        private final List<KeyConfig> keyConfigs;

        public Supplier(CircuitBreaker breaker, List<KeyConfig> keyConfig) {
            this.breaker = breaker;
            this.keyConfigs = keyConfig;
        }

        @Override
        protected SharedMinCompetitive build() {
            return new SharedMinCompetitive(breaker, keyConfigs, this);
        }

        public List<KeyConfig> keyConfigs() {
            return keyConfigs;
        }
    }

    private final BreakingBytesRefBuilder value;
    private final List<KeyConfig> keyConfig;

    /**
     * Bumped under the {@link #value} monitor on every accepted {@link #offer}. Lets
     * {@link #minCompetitiveValue()} cache the decoded bound and skip the copy+decode work while the
     * bound is unchanged. {@link #cachedBound} stays {@code null} until the first decode.
     */
    private long generation;
    private volatile DecodedBound cachedBound;

    /**
     * Set once the source can be abandoned entirely: under {@code NULLS FIRST} the top-K is full of
     * nulls, so no non-null row can ever be competitive. Mirrors
     * {@link SharedNumericThreshold#noFurtherCandidates()}.
     */
    private volatile boolean noFurtherCandidates;

    private record DecodedBound(long generation, @Nullable BytesRef value) {}

    private SharedMinCompetitive(CircuitBreaker breaker, List<KeyConfig> keyConfig, Supplier supplier) {
        super(supplier);
        this.value = new BreakingBytesRefBuilder(breaker, "min_competitive");
        this.keyConfig = keyConfig;
    }

    public List<KeyConfig> configs() {
        return keyConfig;
    }

    /**
     * Offer an update to the min competitive value.
     * @param minCompetitive if it is accepted then the bytes are copied
     * @return whether the update was accepted. {@code false} here means the minimum
     *         value in the local top n is greater than or equal to the minimum
     *         competitive value already recorded
     */
    public boolean offer(BytesRef minCompetitive) {
        synchronized (value) {
            if (value.length() > 0 && value.bytesRefView().compareTo(minCompetitive) <= 0) {
                return false;
            }
            value.clear();
            value.append(minCompetitive);
            generation++;
            return true;
        }
    }

    /**
     * Signal that the source feeding this channel can stop entirely: no row it could still produce
     * can be competitive. The generic {@link TopNOperator} calls this for a single-key
     * {@code NULLS FIRST} sort once its top-K heap is saturated with nulls.
     */
    public void markNoFurtherCandidates() {
        noFurtherCandidates = true;
    }

    public boolean noFurtherCandidates() {
        return noFurtherCandidates;
    }

    /**
     * Read the min competitive value. This will return {@code null} if there
     * isn't yet a min competitive value. Otherwise, this will return a
     * {@link Page} that contains single-position, single-valued {@link Block}s.
     */
    @Nullable
    public Page get(BlockFactory blockFactory) {
        try (BreakingBytesRefBuilder copy = new BreakingBytesRefBuilder(blockFactory.breaker(), "min_competitive_copy")) {
            synchronized (value) {
                if (value.bytesRefView().length == 0) {
                    // Not assigned anything yet
                    return null;
                }
                copy.append(value.bytesRefView());
            }
            ResultBuilder[] builders = new ResultBuilder[keyConfig.size()];
            BytesRef shallow = copy.bytesRefView();
            try {
                for (int i = 0; i < builders.length; i++) {
                    KeyConfig config = keyConfig.get(i);
                    ResultBuilder builder = ResultBuilder.resultBuilderFor(blockFactory, config.elementType, config.encoder, true, 1);
                    builders[i] = builder;
                    if (shallow.bytes[shallow.offset] == (config.nullsFirst ? SMALL_NULL : BIG_NULL)) {
                        builder.decodeValue(new BytesRef(new byte[] { 0 }));
                        shallow.offset += 1;
                        shallow.length -= 1;
                        continue;
                    }
                    shallow.offset += 1;
                    shallow.length -= 1;
                    builder.decodeKey(shallow, config.asc());
                    builder.decodeValue(new BytesRef(new byte[] { 1 }));
                }
                return new Page(ResultBuilder.buildAll(builders));
            } finally {
                Releasables.close(builders);
            }
        }
    }

    /**
     * Decode the competitive bound of a <strong>single</strong> sort key into its raw
     * {@link BytesRef} form, suitable for comparing directly against a format reader's column
     * min/max statistics.
     * <p>
     * Returns {@code null} when nothing has been offered yet, or when the most-competitive row's
     * key is itself {@code null} — in that case there is no usable byte bound, because every
     * non-null value may still be competitive (under NULLS LAST) and we must not skip any range.
     * <p>
     * The returned {@link BytesRef} is an independent, read-only view over an immutable byte array
     * that is never mutated in place. Callers may safely retain it after this method returns and
     * across subsequent {@link #offer} calls from other threads (a new offer publishes a fresh array
     * and leaves earlier ones untouched), but must not mutate it: concurrent readers may view the
     * same backing bytes.
     * <p>
     * Only valid when this channel tracks exactly one key; a multi-key TopN does not expose a
     * single comparable bound.
     */
    @Nullable
    public BytesRef minCompetitiveValue() {
        assert keyConfig.size() == 1 : "minCompetitiveValue is only defined for single-key TopN, got " + keyConfig.size() + " keys";
        KeyConfig config = keyConfig.get(0);
        long gen;
        byte[] copy;
        synchronized (value) {
            gen = generation;
            DecodedBound cached = cachedBound;
            if (cached != null && cached.generation == gen) {
                // Hot path: the bound has not changed since we last decoded it. Hand back a fresh
                // view over the cached, never-mutated bytes so concurrent readers stay independent.
                return view(cached.value);
            }
            int length = value.length();
            if (length == 0) {
                // Nothing offered yet.
                copy = null;
            } else {
                // Decoding munges the bytes in place (see TopNEncoder#decodeBytesRef), so work on a
                // private copy and never touch the shared buffer that other operators keep updating.
                copy = Arrays.copyOfRange(value.bytes(), 0, length);
            }
        }
        BytesRef decoded = decode(config, copy);
        // Publish the decoded bound for this generation. A benign race (two readers decoding the same
        // generation) just recomputes an identical value; the cached bytes are immutable once stored.
        cachedBound = new DecodedBound(gen, decoded);
        return view(decoded);
    }

    @Nullable
    private static BytesRef decode(KeyConfig config, @Nullable byte[] copy) {
        if (copy == null) {
            return null;
        }
        BytesRef encoded = new BytesRef(copy, 0, copy.length);
        // The first byte is the per-key marker written by the KeyExtractor: the "null" sentinel for
        // the configured nulls position, otherwise the "non-null" marker. A null most-competitive
        // key yields no usable byte bound.
        byte nullMarker = config.nullsFirst() ? SMALL_NULL : BIG_NULL;
        if (encoded.bytes[encoded.offset] == nullMarker) {
            return null;
        }
        encoded.offset += 1;
        encoded.length -= 1;
        BytesRef decoded = config.encoder().toSortable(config.asc()).decodeBytesRef(encoded, new BytesRef());
        // Compact to exact bytes so the cached array can be shared read-only across reader threads.
        return BytesRef.deepCopyOf(decoded);
    }

    @Nullable
    private static BytesRef view(@Nullable BytesRef cached) {
        return cached == null ? null : new BytesRef(cached.bytes, cached.offset, cached.length);
    }

    @Override
    protected void closeSideChannel() {
        value.close();
    }
}
