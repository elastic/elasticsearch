/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;

public interface AggregatorFunction extends Releasable {

    void addRawInput(Page page);

    void addIntermediateInput(Page page);

    void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext);

    void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext);

    /** The number of blocks used by intermediate state. */
    int intermediateBlockCount();
}
