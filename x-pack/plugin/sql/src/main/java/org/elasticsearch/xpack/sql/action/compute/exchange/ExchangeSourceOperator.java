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

public class ExchangeSourceOperator implements Operator {

    private final ExchangeSource source;
    private ListenableActionFuture<Void> isBlocked = NOT_BLOCKED;

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
    public boolean needsInput() {
        return false;
    }

    @Override
    public void addInput(Page page) {
        throw new UnsupportedOperationException();
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
