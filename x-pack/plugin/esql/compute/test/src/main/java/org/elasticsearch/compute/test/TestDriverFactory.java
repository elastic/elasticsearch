/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.util.List;

public class TestDriverFactory {

    /**
     * This is a convenience factory to create a test driver instance
     * that accepts fewer parameters
     */
    public static Driver create(
        DriverContext driverContext,
        SourceOperator source,
        List<Operator> intermediateOperators,
        SinkOperator sink
    ) {
        return create(driverContext, source, intermediateOperators, sink, () -> {});
    }

    public static Driver create(
        DriverContext driverContext,
        SourceOperator source,
        List<Operator> intermediateOperators,
        SinkOperator sink,
        Releasable releasable
    ) {
        // Do not wrap the local breaker for small local breakers, as the output mights not match expectations.
        if (driverContext.breaker() instanceof CrankyCircuitBreakerService.CrankyCircuitBreaker == false
            && driverContext.breaker() instanceof LocalCircuitBreaker == false
            && driverContext.breaker().getLimit() >= ByteSizeValue.ofMb(100).getBytes()
            && Randomness.get().nextBoolean()) {
            final int overReservedBytes = Randomness.get().nextInt(1024 * 1024);
            final int maxOverReservedBytes = overReservedBytes + Randomness.get().nextInt(1024 * 1024);
            var localBreaker = new LocalCircuitBreaker(driverContext.breaker(), overReservedBytes, maxOverReservedBytes);
            BlockFactory localBlockFactory = driverContext.blockFactory().newChildFactory(localBreaker);
            driverContext = new DriverContext(localBlockFactory.bigArrays(), localBlockFactory);
        }
        if (driverContext.breaker() instanceof LocalCircuitBreaker localBreaker) {
            releasable = Releasables.wrap(releasable, localBreaker);
        }
        return new Driver(
            "unset",
            "test-task",
            "unset",
            "unset",
            System.currentTimeMillis(),
            System.nanoTime(),
            driverContext,
            () -> null,
            source,
            intermediateOperators,
            sink,
            Driver.DEFAULT_STATUS_INTERVAL,
            releasable
        );
    }
}
