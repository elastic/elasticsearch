/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.topn.TopNOperator;

/**
 * "Modes" for running an aggregate function.
 * <p>
 *     Aggregations running on a single "stream" of {@link Block}s should run in
 *     {@link #SINGLE} mode. This works for aggs that come after a
 *     {@link TopNOperator} or another agg.
 * </p>
 * <p>
 *     But all other aggregations run distributed. On many threads on each data node
 *     we run in {@link #INITIAL} mode to consume raw data and output just enough to
 *     finish the job later. All threads on a node dump the data into the same agg
 *     run in {@link #INTERMEDIATE} mode to perform "node reduction". Then, on the
 *     coordinating node, the outputs of the "node reduction" goes into the agg in
 *     {@link #FINAL} mode.
 * </p>
 * <p>
 *     Put another way, all data must flow throw aggregations in one of these two sequences:
 * </p>
 * <ul>
 *     <li>{@link #SINGLE}</li>
 *     <li>{@link #INITIAL} {@link #INTERMEDIATE}* {@link #FINAL}</li>
 * </ul>
 */
public enum AggregatorMode {

    /**
     * Maps raw inputs to intermediate outputs.
     */
    INITIAL(false, true),

    /**
     * Maps intermediate inputs to intermediate outputs.
     */
    INTERMEDIATE(true, true),

    /**
     * Maps intermediate inputs to final outputs.
     */
    FINAL(true, false),

    /**
     * Maps raw inputs to final outputs. Most useful for testing.
     */
    SINGLE(false, false);

    private final boolean inputPartial;

    private final boolean outputPartial;

    AggregatorMode(boolean inputPartial, boolean outputPartial) {
        this.inputPartial = inputPartial;
        this.outputPartial = outputPartial;
    }

    public boolean isInputPartial() {
        return inputPartial;
    }

    public boolean isOutputPartial() {
        return outputPartial;
    }
}
