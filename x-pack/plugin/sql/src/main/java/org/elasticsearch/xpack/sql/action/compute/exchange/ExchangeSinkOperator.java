/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.xpack.sql.action.compute.Operator;
import org.elasticsearch.xpack.sql.action.compute.Page;

public class ExchangeSinkOperator implements Operator {

    private final ExchangeSink sink;

    private ListenableActionFuture<Void> isBlocked = NOT_BLOCKED;

    public ExchangeSinkOperator(ExchangeSink sink) {
        this.sink = sink;
    }

    @Override
    public Page getOutput() {
        return null;
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
    public ListenableActionFuture<Void> isBlocked()
    {
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
