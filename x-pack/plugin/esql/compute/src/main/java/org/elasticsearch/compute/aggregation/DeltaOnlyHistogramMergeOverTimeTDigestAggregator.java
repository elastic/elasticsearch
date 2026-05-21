/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;

/**
 * Grouping aggregator for histogram_merge_over_time with TDigest values.
 * Delegates to {@link HistogramMergeTDigestAggregator} for the actual merge,
 * but reads temporality from an additional channel and defaults null to {@link Temporality#DELTA}.
 * Cumulative temporality is unsupported and produces a warning; affected values are skipped.
 */
@GroupingAggregator(
    value = { @IntermediateState(name = "value", type = "TDIGEST"), @IntermediateState(name = "seen", type = "BOOLEAN") },
    processNulls = true
)
public class DeltaOnlyHistogramMergeOverTimeTDigestAggregator {

    public static final String CUMULATIVE_UNSUPPORTED_WARNING = "Cumulative temporality is not supported for the tdigest type."
        + " The affected time series are excluded from the aggregation.";

    public static TemporalityAwareTDigestGroupingState initGrouping(BigArrays bigArrays, DriverContext driverContext, Warnings warnings) {
        return new TemporalityAwareTDigestGroupingState(bigArrays, driverContext, warnings);
    }

    public static void combine(
        TemporalityAwareTDigestGroupingState current,
        int groupId,
        TDigestHolder value,
        @Position int position,
        BytesRefBlock temporality
    ) {
        if (current.cachedTemporalityAccessor == null || current.cachedTemporalityAccessor.block() != temporality) {
            current.cachedTemporalityAccessor = TemporalityAccessor.create(temporality, Temporality.DELTA);
            assert current.cachedTemporalityAccessor.block() == temporality;
        }
        try {
            if (current.cachedTemporalityAccessor.get(position) == Temporality.DELTA) {
                current.delegate.add(groupId, value);
            } else {
                current.warnings.registerException(IllegalArgumentException.class, CUMULATIVE_UNSUPPORTED_WARNING);
            }
        } catch (InvalidTemporalityException e) {
            current.warnings.registerException(e);
        }
    }

    public static void combineIntermediate(TemporalityAwareTDigestGroupingState current, int groupId, TDigestHolder value, boolean seen) {
        if (seen) {
            current.delegate.add(groupId, value);
        }
    }

    public static Block evaluateFinal(
        TemporalityAwareTDigestGroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.delegate.evaluateFinal(selected, ctx.driverContext());
    }

    /**
     * Wraps {@link TDigestStates.GroupingState} to carry temporality accessor and warnings.
     */
    public static final class TemporalityAwareTDigestGroupingState implements Releasable, GroupingAggregatorState {
        private final TDigestStates.GroupingState delegate;
        private TemporalityAccessor cachedTemporalityAccessor;
        private final Warnings warnings;

        private TemporalityAwareTDigestGroupingState(BigArrays bigArrays, DriverContext driverContext, Warnings warnings) {
            this.delegate = new TDigestStates.GroupingState(bigArrays, driverContext.breaker());
            this.warnings = warnings;
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            delegate.toIntermediate(blocks, offset, selected, driverContext);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            delegate.enableGroupIdTracking(seenGroupIds);
        }
    }
}
