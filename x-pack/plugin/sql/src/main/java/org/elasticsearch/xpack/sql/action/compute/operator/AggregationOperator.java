/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator;

import org.elasticsearch.xpack.sql.action.compute.aggregation.Aggregator;
import org.elasticsearch.xpack.sql.action.compute.data.Block;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Blocking aggregation operator.  An aggregation operator aggregates its input with one or more
 * aggregator functions, e.g. avg, max, etc, and outputs a Page containing the results of those
 * aggregations.
 *
 * The operator is blocking in the sense that it only produces output once all possible input has
 * been added, that is, when the {@link #finish} method has been called.
 */
public class AggregationOperator implements Operator {

    // monotonically increasing state
    private static final int NEEDS_INPUT = 0;
    private static final int HAS_OUTPUT = 1;
    private static final int FINISHING = 2;
    private static final int FINISHED = 3;

    private int state;

    private final List<Aggregator> aggregators;

    public AggregationOperator(List<Aggregator> aggregators) {
        Objects.requireNonNull(aggregators);
        checkNonEmpty(aggregators);
        this.aggregators = aggregators;
        state = NEEDS_INPUT;
    }

    @Override
    public boolean needsInput() {
        return state == NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page) {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");
        for (Aggregator aggregator : aggregators) {
            aggregator.processPage(page);
        }
    }

    @Override
    public Page getOutput() {
        if (state != HAS_OUTPUT) {
            return null;
        }

        Block[] blocks = new Block[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            var aggregator = aggregators.get(i);
            blocks[i] = aggregator.evaluate();
        }

        Page page = new Page(blocks);
        state = FINISHED;
        return page;
    }

    @Override
    public void finish() {
        if (state == NEEDS_INPUT) {
            state = HAS_OUTPUT;
        }
    }

    @Override
    public boolean isFinished() {
        return state == FINISHED;
    }

    @Override
    public void close() {}

    private static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    private static void checkNonEmpty(List<?> list) {
        if (list.size() < 1) {
            throw new IllegalArgumentException("empty list");
        }
    }
}
