/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;

public class TestBlockFactory {

    private static final BlockFactory NON_BREAKING = BlockFactory.getInstance(
        new NoopCircuitBreaker("test-noop"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    /**
     * Returns the Non-Breaking block factory.
     */
    public static BlockFactory getNonBreakingInstance() {
        return NON_BREAKING;
    }
}
