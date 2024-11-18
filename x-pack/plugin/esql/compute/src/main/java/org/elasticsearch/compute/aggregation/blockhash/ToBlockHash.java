/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.BlockFactory;

/**
 * Used by grouping functions to provide a custom {@link org.elasticsearch.compute.aggregation.blockhash.BlockHash} implementation.
 */
public interface ToBlockHash {
    BlockHash toBlockHash(BlockFactory blockFactory, int channel, AggregatorMode aggregatorMode);
}
