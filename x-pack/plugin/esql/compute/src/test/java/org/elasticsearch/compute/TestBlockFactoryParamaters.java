/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactoryParameters;

public class TestBlockFactoryParamaters implements BlockFactoryParameters {
    @Override
    public CircuitBreaker breaker() {
        return new NoopCircuitBreaker("ESQL-test-breaker");
    }

    @Override
    public BigArrays bigArrays() {
        return BigArrays.NON_RECYCLING_INSTANCE;
    }
}
