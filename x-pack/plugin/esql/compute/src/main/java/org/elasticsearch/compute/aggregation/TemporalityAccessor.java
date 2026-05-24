/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.core.Nullable;

import java.util.function.Function;

/**
 * Helper class for efficiently resolving metric temporality (CUMULATIVE or DELTA) from a {@link BytesRefBlock}.
 * Optimizes for common access patterns by detecting constant blocks, ordinal-based blocks, or falling back
 * to dynamic per-position lookups.
 */
public class TemporalityAccessor {

    // Visible for testing
    enum Mode {
        CONSTANT,
        ORDINAL,
        DYNAMIC
    }

    // Visible for testing
    final Mode mode;
    /**
     * For {@code mode == CONSTANT} this stores the constant temporality, which will be returned by {@link #get(int)}.
     * For all other modes this stores the default temporality to use in case null values are encountered.
     */
    private final Temporality constantOrDefaultTemporality;
    private final BytesRefBlock temporalityBlock;
    private final Function<BytesRef, Temporality> invalidTemporalityHandler;

    private final BytesRef scratch;

    private int cachedDeltaOrdinal = -1;
    private int cachedCumulativeOrdinal = -1;

    /**
     * Creates an accessor that always returns the given constant temporality.
     */
    public static TemporalityAccessor constant(Temporality constantTemporality) {
        return new TemporalityAccessor(Mode.CONSTANT, constantTemporality, null, null, null);
    }

    private static TemporalityAccessor constantWithBlock(Temporality constantTemporality, BytesRefBlock temporalityBlock) {
        return new TemporalityAccessor(Mode.CONSTANT, constantTemporality, temporalityBlock, null, null);
    }

    /**
     * Creates an accessor for the given temporality block that throws {@link InvalidTemporalityException}
     * when an unrecognized temporality value is encountered.
     *
     * @param temporalityBlock the block containing temporality values as BytesRef strings ("cumulative" or "delta")
     * @param defaultTemporality the temporality to use for null values
     */
    public static TemporalityAccessor create(BytesRefBlock temporalityBlock, Temporality defaultTemporality) {
        return create(temporalityBlock, defaultTemporality, v -> { throw new InvalidTemporalityException(v); });
    }

    /**
     * Creates an accessor for the given temporality block, automatically selecting the most efficient access mode.
     *
     * @param temporalityBlock the block containing temporality values as BytesRef strings ("cumulative" or "delta")
     * @param defaultTemporality the temporality to use for null values
     * @param invalidTemporalityHandler called when an unrecognized temporality value is encountered
     */
    public static TemporalityAccessor create(
        BytesRefBlock temporalityBlock,
        Temporality defaultTemporality,
        Function<BytesRef, Temporality> invalidTemporalityHandler
    ) {
        if (temporalityBlock.areAllValuesNull() || temporalityBlock.getPositionCount() == 0) {
            return constantWithBlock(defaultTemporality, temporalityBlock);
        }
        BytesRef scratch = new BytesRef();
        if (temporalityBlock.asVector() != null && temporalityBlock.asVector().isConstant()) {
            BytesRef constantValue = temporalityBlock.asVector().getBytesRef(0, scratch);
            return switch (resolveTemporality(constantValue)) {
                case null -> new TemporalityAccessor(
                    Mode.DYNAMIC,
                    defaultTemporality,
                    temporalityBlock,
                    scratch,
                    invalidTemporalityHandler
                );
                case CUMULATIVE -> constantWithBlock(Temporality.CUMULATIVE, temporalityBlock);
                case DELTA -> constantWithBlock(Temporality.DELTA, temporalityBlock);
            };
        } else if (temporalityBlock.asOrdinals() != null) {
            return new TemporalityAccessor(Mode.ORDINAL, defaultTemporality, temporalityBlock, scratch, invalidTemporalityHandler);
        } else {
            return new TemporalityAccessor(Mode.DYNAMIC, defaultTemporality, temporalityBlock, scratch, invalidTemporalityHandler);
        }
    }

    public TemporalityAccessor(
        Mode mode,
        Temporality constantOrDefaultTemporality,
        @Nullable BytesRefBlock temporalityBlock,
        @Nullable BytesRef scratch,
        @Nullable Function<BytesRef, Temporality> invalidTemporalityHandler
    ) {
        assert temporalityBlock != null || mode == Mode.CONSTANT : "block must be provided if the mode is not constant";
        assert scratch != null || mode == Mode.CONSTANT : "scratch must be provided if the mode is not constant";
        assert invalidTemporalityHandler != null || mode == Mode.CONSTANT
            : "invalidTemporalityHandler must be provided if the mode is not constant";
        this.mode = mode;
        this.constantOrDefaultTemporality = constantOrDefaultTemporality;
        this.temporalityBlock = temporalityBlock;
        this.scratch = scratch;
        this.invalidTemporalityHandler = invalidTemporalityHandler;
    }

    public BytesRefBlock block() {
        return temporalityBlock;
    }

    /**
     * Returns the temporality for the given position.
     */
    public Temporality get(int position) {
        return switch (mode) {
            case CONSTANT -> constantOrDefaultTemporality;
            case ORDINAL -> getOrdinalBased(position);
            case DYNAMIC -> getDynamic(position);
        };
    }

    private Temporality getOrdinalBased(int position) {
        OrdinalBytesRefBlock ordinalBytesRefBlock = temporalityBlock.asOrdinals();
        assert ordinalBytesRefBlock != null : "ordinal mode requires an ordinals block";
        if (ordinalBytesRefBlock.isNull(position)) {
            return constantOrDefaultTemporality;
        }
        IntBlock ordinals = ordinalBytesRefBlock.getOrdinalsBlock();

        int ordinal = ordinals.getInt(ordinals.getFirstValueIndex(position));
        if (ordinal == cachedCumulativeOrdinal) {
            return Temporality.CUMULATIVE;
        }
        if (ordinal == cachedDeltaOrdinal) {
            return Temporality.DELTA;
        }

        BytesRef temporalityBytesRef = ordinalBytesRefBlock.getDictionaryVector().getBytesRef(ordinal, scratch);
        Temporality resolved = resolveTemporality(temporalityBytesRef);
        if (resolved == null) {
            return invalidTemporalityHandler.apply(temporalityBytesRef);
        }
        if (resolved == Temporality.CUMULATIVE) {
            cachedCumulativeOrdinal = ordinal;
        }
        if (resolved == Temporality.DELTA) {
            cachedDeltaOrdinal = ordinal;
        }
        return resolved;
    }

    private Temporality getDynamic(int position) {
        if (temporalityBlock.isNull(position)) {
            return constantOrDefaultTemporality;
        }
        BytesRef value = temporalityBlock.getBytesRef(temporalityBlock.getFirstValueIndex(position), scratch);
        Temporality resolved = resolveTemporality(value);
        if (resolved != null) {
            return resolved;
        } else {
            return invalidTemporalityHandler.apply(value);
        }
    }

    @Nullable
    private static Temporality resolveTemporality(BytesRef value) {
        if (Temporality.CUMULATIVE.bytesRef().equals(value)) {
            return Temporality.CUMULATIVE;
        } else if (Temporality.DELTA.bytesRef().equals(value)) {
            return Temporality.DELTA;
        }
        return null;
    }
}
