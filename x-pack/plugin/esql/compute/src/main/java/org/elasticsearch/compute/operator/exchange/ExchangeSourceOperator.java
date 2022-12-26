/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;

/**
 * Source operator implementation that retrieves data from an {@link ExchangeSource}
 */
@Experimental
public class ExchangeSourceOperator extends SourceOperator {

    private final ExchangeSource source;
    private ListenableActionFuture<Void> isBlocked = NOT_BLOCKED;

    public record ExchangeSourceOperatorFactory(Exchange exchange) implements SourceOperatorFactory {

        @Override
        public SourceOperator get() {
            return new ExchangeSourceOperator(exchange.getNextSource());
        }

        @Override
        public String describe() {
            return "ExchangeSourceOperator(partitioning = " + exchange.partitioning() + ")";
        }
    }

    public ExchangeSourceOperator(ExchangeSource source) {
        this.source = source;
    }

    @Override
    public Page getOutput() {
        return source.removePage();
    }

    @Override
    public boolean isFinished() {
        return source.isFinished();
    }

    @Override
    public void finish() {
        source.finish();
    }

    @Override
    public ListenableActionFuture<Void> isBlocked() {
        if (isBlocked.isDone()) {
            isBlocked = source.waitForReading();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public void close() {
        source.close();
    }
}
