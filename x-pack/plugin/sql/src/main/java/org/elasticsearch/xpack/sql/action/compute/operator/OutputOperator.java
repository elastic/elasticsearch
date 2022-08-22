/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator;

import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Sink operator that calls a given listener for each page received. The listener receives both the page as well as schema information,
 * i.e. the names of the rows that are outputted.
 */
public class OutputOperator implements Operator {

    private final List<String> columns;
    private final BiConsumer<List<String>, Page> pageConsumer;

    public OutputOperator(List<String> columns, BiConsumer<List<String>, Page> pageConsumer) {
        this.columns = columns;
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
        pageConsumer.accept(columns, page);
    }

    @Override
    public void close() {

    }
}
