/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

@Experimental
public interface GroupingAggregatorFunction extends Releasable {

    void addRawInput(LongBlock groupIdBlock, Page page);

    void addRawInput(LongVector groupIdVector, Page page);

    void addIntermediateInput(LongVector groupIdVector, Page page);

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
    void evaluateFinal(Block[] blocks, int offset, IntVector selected);

    /** The number of blocks used by intermediate state. */
    int intermediateBlockCount();
}
