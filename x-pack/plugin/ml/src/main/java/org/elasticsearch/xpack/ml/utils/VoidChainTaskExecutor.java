/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;

import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

/**
 * A utility that allows chained (serial) execution of a number of tasks
 * in async manner.
 */
public class VoidChainTaskExecutor extends TypedChainTaskExecutor<Void> {

    public VoidChainTaskExecutor(ExecutorService executorService, boolean shortCircuit) {
        this(executorService, (a) -> true, (e) -> shortCircuit);
    }

    VoidChainTaskExecutor(ExecutorService executorService,
                          Predicate<Void> continuationPredicate,
                          Predicate<Exception> failureShortCircuitPredicate) {
        super(executorService, continuationPredicate, failureShortCircuitPredicate);
    }
}
