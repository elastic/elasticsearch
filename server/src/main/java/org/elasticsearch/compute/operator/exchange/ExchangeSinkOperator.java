/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SinkOperator;

/**
 * Sink operator implementation that pushes data to an {@link ExchangeSink}
 */
@Experimental
public class ExchangeSinkOperator extends SinkOperator {

    private final ExchangeSink sink;

    private ListenableActionFuture<Void> isBlocked = NOT_BLOCKED;

    public record ExchangeSinkOperatorFactory(Exchange ex) implements SinkOperatorFactory {

        public SinkOperator get() {
            return new ExchangeSinkOperator(ex.createSink());
        }

        @Override
        public String describe() {
            return "ExchangeSinkOperator";
        }
    }

    public ExchangeSinkOperator(ExchangeSink sink) {
        this.sink = sink;
    }

    @Override
    public boolean isFinished() {
        return sink.isFinished();
    }

    @Override
    public void finish() {
        sink.finish();
    }

    @Override
    public ListenableActionFuture<Void> isBlocked() {
        if (isBlocked.isDone()) {
            isBlocked = sink.waitForWriting();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean needsInput() {
        return isFinished() == false && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page) {
        sink.addPage(page);
    }

    @Override
    public void close() {
        finish();
    }
}
