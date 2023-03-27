/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;

/**
 * Abstract superclass for operators that accept a single page, modify it, and then return it.
 */
public abstract class AbstractPageMappingOperator implements Operator {
    private Page prev;
    private boolean finished = false;

    protected abstract Page process(Page page);

    @Override
    public abstract String toString();

    @Override
    public final boolean needsInput() {
        return prev == null && finished == false;
    }

    @Override
    public final void addInput(Page page) {
        prev = page;
    }

    @Override
    public final void finish() {
        finished = true;
    }

    @Override
    public final boolean isFinished() {
        return finished && prev == null;
    }

    @Override
    public final Page getOutput() {
        if (prev == null) {
            return null;
        }
        Page p = process(prev);
        prev = null;
        return p;
    }

    @Override
    public final void close() {}
}
