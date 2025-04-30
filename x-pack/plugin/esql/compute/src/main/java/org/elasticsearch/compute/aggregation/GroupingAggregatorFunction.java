/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantNullBlock;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.IntVectorBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.core.Releasable;

/**
 * Applies some grouping function like {@code min} or {@code avg} to many values,
 * grouped into buckets.
 */
public interface GroupingAggregatorFunction extends Releasable {
    /**
     * Consume group ids to cause the {@link GroupingAggregatorFunction}
     * to group values at a particular position into a particular group.
     */
    interface AddInput extends Releasable {
        /**
         * Send a batch of group ids to the aggregator. The {@code groupIds}
         * may be offset from the start of the block to allow for sending chunks
         * of group ids.
         * <p>
         *     Any single position may be collected into arbitrarily many group
         *     ids. Often it's just one, but it's quite possible for a single
         *     position to be collected into thousands or millions of group ids.
         *     The {@code positionOffset} controls the start of the chunk of group
         *     ids contained in {@code groupIds}.
         * </p>
         * <p>
         *     It is possible for an input position to be cut into more than one
         *     chunk. In other words, it's possible for this method to be called
         *     multiple times with the same {@code positionOffset} and a
         *     {@code groupIds} {@linkplain Block} that contains thousands of
         *     values at a single positions.
         * </p>
         * <p>
         *     Finally, it's possible for a single position to be collected into
         *     <strong>groupIds</strong>. In that case it's positionOffset may
         *     be skipped entirely or the groupIds block could contain a
         *     {@code null} value at that position.
         * </p>
         * <p>
         *     This method delegates the processing to the other overloads for specific groupIds block types.
         * </p>
         * @param positionOffset offset into the {@link Page} used to build this
         *                       {@link AddInput} of these ids
         * @param groupIds {@link Block} of group id, some of which may be null
         *                 or multivalued
         */
        default void add(int positionOffset, IntBlock groupIds) {
            switch (groupIds) {
                case ConstantNullBlock ignored:
                    // No-op
                    break;
                case IntVectorBlock b:
                    add(positionOffset, b.asVector());
                    break;
                case IntArrayBlock b:
                    add(positionOffset, b);
                    break;
                case IntBigArrayBlock b:
                    add(positionOffset, b);
                    break;
            }
        }

        /**
         * Implementation of {@link #add(int, IntBlock)} for a specific type of block.
         */
        void add(int positionOffset, IntArrayBlock groupIds);

        /**
         * Implementation of {@link #add(int, IntBlock)} for a specific type of block.
         */
        void add(int positionOffset, IntBigArrayBlock groupIds);

        /**
         * Send a batch of group ids to the aggregator. The {@code groupIds}
         * may be offset from the start of the block to allow for sending chunks
         * of group ids.
         * <p>
         *     See {@link #add(int, IntBlock)} for discussion on the offset. This
         *     method can only be called with blocks contained in a {@link Vector}
         *     which only allows a single value per position.
         * </p>
         * @param positionOffset offset into the {@link Page} used to build this
         *                       {@link AddInput} of these ids
         * @param groupIds {@link Vector} of group id, some of which may be null
         *                 or multivalued
         */
        void add(int positionOffset, IntVector groupIds);
    }

    /**
     * Prepare to process a single page of input.
     * <p>
     *     This should load the input {@link Block}s and check their types and
     *     select an optimal path and return that path as an {@link AddInput}.
     * </p>
     */
    AddInput prepareProcessPage(SeenGroupIds seenGroupIds, Page page);  // TODO allow returning null to opt out of the callback loop

    /**
     * Call this to signal to the aggregation that the {@code selected}
     * parameter that's passed to {@link #evaluateIntermediate} or
     * {@link #evaluateFinal} may reference groups that haven't been
     * seen. This puts the underlying storage into a mode where it'll
     * track which group ids have been seen, even if that increases the
     * overhead.
     */
    void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds);

    /**
     * Add data produced by {@link #evaluateIntermediate}.
     */
    void addIntermediateInput(int positionOffset, IntVector groupIdVector, Page page);

    /**
     * Add the position-th row from the intermediate output of the given aggregator function to the groupId
     */
    void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position);

    /**
     * Build the intermediate results for this aggregation.
     * @param selected the groupIds that have been selected to be included in
     *                 the results. Always ascending.
     */
    void evaluateIntermediate(Block[] blocks, int offset, IntVector selected);

    /**
     * Build the final results for this aggregation.
     * @param selected the groupIds that have been selected to be included in
     *                 the results. Always ascending.
     */
    void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evaluationContext);

    /** The number of blocks used by intermediate state. */
    int intermediateBlockCount();
}
