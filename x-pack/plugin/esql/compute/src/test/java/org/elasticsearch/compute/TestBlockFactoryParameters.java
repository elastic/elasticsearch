/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactoryParameters;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBlockFactoryParameters implements BlockFactoryParameters {

    final CircuitBreaker breaker;
    final BigArrays bigArrays;

    public TestBlockFactoryParameters() {
        breaker = new MockBigArrays.LimitedBreaker("esql-test-breaker", ByteSizeValue.ofGb(1));
        var breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService);
    }

    @Override
    public CircuitBreaker breaker() {
        return breaker;
    }

    @Override
    public BigArrays bigArrays() {
        return bigArrays;
    }
}
