/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactoryParameters;

/** A provider for sharing the given parameters with the compute engine's block factory. */
public class EsqlBlockFactoryParams implements BlockFactoryParameters {

    static final CircuitBreaker NOOP_BREAKER = new NoopCircuitBreaker("ESQL-noop-breaker");

    static CircuitBreaker ESQL_BREAKER;
    static BigArrays ESQL_BIGARRAYS;

    static void init(BigArrays bigArrays) {
        ESQL_BREAKER = bigArrays.breakerService().getBreaker("request");
        ESQL_BIGARRAYS = bigArrays;
    }

    final CircuitBreaker breaker;
    final BigArrays bigArrays;

    public EsqlBlockFactoryParams() {
        this.breaker = ESQL_BREAKER;
        this.bigArrays = ESQL_BIGARRAYS;
    }

    @Override
    public CircuitBreaker breaker() {
        return breaker != null ? breaker : NOOP_BREAKER;
    }

    @Override
    public BigArrays bigArrays() {
        return bigArrays != null ? bigArrays : BigArrays.NON_RECYCLING_INSTANCE;
    }
}
