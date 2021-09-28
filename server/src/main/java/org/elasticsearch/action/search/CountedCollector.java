/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;

/**
 * This is a simple base class to simplify fan out to shards and collect their results. Each results passed to
 * {@link #onResult(SearchPhaseResult)} will be set to the provided result array
 * where the given index is used to set the result on the array.
 */
final class CountedCollector<R extends SearchPhaseResult> {
    private final ArraySearchPhaseResults<R> resultConsumer;
    private final CountDown counter;
    private final Runnable onFinish;
    private final SearchPhaseContext context;

    CountedCollector(ArraySearchPhaseResults<R> resultConsumer, int expectedOps, Runnable onFinish, SearchPhaseContext context) {
        this.resultConsumer = resultConsumer;
        this.counter = new CountDown(expectedOps);
        this.onFinish = onFinish;
        this.context = context;
    }

    /**
     * Forcefully counts down an operation and executes the provided runnable
     * if all expected operations where executed
     */
    void countDown() {
        assert counter.isCountedDown() == false : "more operations executed than specified";
        if (counter.countDown()) {
            onFinish.run();
        }
    }

    /**
     * Sets the result to the given array index and then runs {@link #countDown()}
     */
    void onResult(R result) {
        resultConsumer.consumeResult(result,  this::countDown);
    }

    /**
     * Escalates the failure via {@link SearchPhaseContext#onShardFailure(int, SearchShardTarget, Exception)}
     * and then runs {@link #countDown()}
     */
    void onFailure(final int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
        try {
            context.onShardFailure(shardIndex, shardTarget, e);
        } finally {
            countDown();
        }
    }
}
