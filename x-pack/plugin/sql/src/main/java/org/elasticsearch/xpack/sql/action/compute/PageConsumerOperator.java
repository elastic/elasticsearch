/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import java.util.function.Consumer;

public class PageConsumerOperator implements Operator {

    private final Consumer<Page> pageConsumer;

    public PageConsumerOperator(Consumer<Page> pageConsumer) {
        this.pageConsumer = pageConsumer;
    }

    boolean finished = false;

    @Override
    public Page getOutput() {
        return null;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        pageConsumer.accept(page);
    }

    @Override
    public void close() {

    }
}
