/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.sql.action.compute.operator.Operator.NOT_BLOCKED;
import static org.elasticsearch.xpack.sql.action.compute.operator.exchange.Exchanger.FINISHED;

/**
 * Sink for exchanging data. Thread-safe.
 */
public class ExchangeSink {

    private final AtomicBoolean finished = new AtomicBoolean();
    private final Consumer<ExchangeSink> onFinish;
    private final Exchanger exchanger;

    public ExchangeSink(Exchanger exchanger, Consumer<ExchangeSink> onFinish) {
        this.exchanger = exchanger;
        this.onFinish = onFinish;
    }

    public static ExchangeSink finishedExchangeSink() {
        ExchangeSink finishedSink = new ExchangeSink(FINISHED, sink -> {});
        finishedSink.finish();
        return finishedSink;
    }

    /**
     * adds a new page to this sink
     */
    public void addPage(Page page) {
        exchanger.accept(page);
    }

    /**
     * called once all pages have been added (see {@link #addPage(Page)}).
     */
    public void finish() {
        if (finished.compareAndSet(false, true)) {
            exchanger.finish();
            onFinish.accept(this);
        }
    }

    /**
     * Whether the sink has received all pages
     */
    public boolean isFinished() {
        return finished.get();
    }

    /**
     * Whether the sink is blocked on adding more pages
     */
    public ListenableActionFuture<Void> waitForWriting() {
        if (isFinished()) {
            return NOT_BLOCKED;
        }
        return exchanger.waitForWriting();
    }

}
