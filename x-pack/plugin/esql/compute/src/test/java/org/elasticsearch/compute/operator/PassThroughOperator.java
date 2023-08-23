/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;

/** An operator that just passes pages through until it is finished. */
public class PassThroughOperator implements Operator {

    boolean finished;
    Page page;

    @Override
    public boolean needsInput() {
        return page == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        assert this.page == null;
        this.page = page;
    }

    @Override
    public Page getOutput() {
        Page p = page;
        page = null;
        return p;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && page == null;
    }

    @Override
    public void close() {}
}
