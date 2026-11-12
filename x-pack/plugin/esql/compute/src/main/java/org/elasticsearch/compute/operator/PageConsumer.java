/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;

import java.util.function.Consumer;

/**
 * An {@link OutputOperator} consumer that participates in the driver lifecycle: it can hold the driver back while work
 * is in flight and remain unfinished after the last page until outstanding work completes.
 */
public interface PageConsumer {

    interface Factory {
        PageConsumer create(DriverContext driverContext);

        static Factory of(Consumer<Page> consumer) {
            return driverContext -> PageConsumer.of(consumer);
        }
    }

    static PageConsumer of(Consumer<Page> consumer) {
        return new PageConsumer() {
            @Override
            public void consume(Page page) {
                consumer.accept(page);
            }

            @Override
            public IsBlockedResult isBlocked() {
                return Operator.NOT_BLOCKED;
            }

            @Override
            public void finish() {}

            @Override
            public boolean isFinished() {
                return true;
            }
        };
    }

    void consume(Page page);

    IsBlockedResult isBlocked();

    void finish();

    boolean isFinished();
}
