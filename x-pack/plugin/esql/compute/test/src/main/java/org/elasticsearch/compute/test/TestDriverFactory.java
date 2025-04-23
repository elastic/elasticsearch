/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasable;

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
