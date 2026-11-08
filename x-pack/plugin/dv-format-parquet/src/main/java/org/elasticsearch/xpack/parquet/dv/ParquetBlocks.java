/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;

/**
 * The block factory that this plugin gives to the ES|QL reader. The blocks are short-lived and copied out at once, and a Lucene thread
 * has no ES|QL driver context to take a breaker from, so the spike uses a factory with no circuit breaker accounting. That is an
 * open risk: nothing here limits the memory that the reader uses for decoding.
 */
public final class ParquetBlocks {

    private static final BlockFactory FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("parquet"))
        .build();

    private ParquetBlocks() {}

    public static BlockFactory factory() {
        return FACTORY;
    }
}
