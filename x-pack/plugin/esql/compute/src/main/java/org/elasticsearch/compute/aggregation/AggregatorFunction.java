/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;

/**
 * A non-grouped aggregation.
 */
public interface AggregatorFunction extends Releasable {
    /**
     * Add a page worth of data to the aggregation.
     * @param mask a mask to apply to the positions. If the position is {@code false} then
     *             the aggregation should skip it.
     */
    void addRawInput(Page page, BooleanVector mask);

    /**
     * Add a pre-aggregated page worth of "intermediate" input. This intermediate input
     * will have been created by calling {@link #evaluateIntermediate} on this agg, though
     * likely in a different {@link Driver} and <strong>maybe</strong> on a different
     * physical node.
     */
    void addIntermediateInput(Page page);

    /**
     * Build pre-aggregated "intermediate" data to pass to the {@link #addIntermediateInput}.
     * @param blocks write the output into this array
     * @param offset write the first {@link Block} at this offset in {@code blocks}
     */
    void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext);

    /**
     * Build the final results from running this agg.
     * @param blocks write the output into this array
     * @param offset write the first {@link Block} at this offset in {@code blocks}
     */
    void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext);

    /** The number of blocks used by intermediate state. */
    int intermediateBlockCount();
}
