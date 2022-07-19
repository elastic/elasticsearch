/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.xpack.sql.action.compute.Page;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.sql.action.compute.Operator.NOT_BLOCKED;

public class ExchangeSink {

    private final AtomicBoolean finished = new AtomicBoolean();
    private final Consumer<ExchangeSink> onFinish;
    private final Exchanger exchanger;

    public ExchangeSink(Exchanger exchanger, Consumer<ExchangeSink> onFinish) {
        this.exchanger = exchanger;
        this.onFinish = onFinish;
    }

    public void finish()
    {
        if (finished.compareAndSet(false, true)) {
            exchanger.finish();
            onFinish.accept(this);
        }
    }

    public boolean isFinished()
    {
        return finished.get();
    }

    public void addPage(Page page)
    {
        exchanger.accept(page);
    }

    public ListenableActionFuture<Void> waitForWriting()
    {
        if (isFinished()) {
            return NOT_BLOCKED;
        }
        return exchanger.waitForWriting();
    }

}
