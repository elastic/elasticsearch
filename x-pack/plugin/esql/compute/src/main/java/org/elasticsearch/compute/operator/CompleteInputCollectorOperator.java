/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A base class that loads in and works across all input pages for an operator
 */
public abstract class CompleteInputCollectorOperator implements Operator {

    protected boolean finished;
    protected final Deque<Page> inputPages = new ArrayDeque<>();
    protected int pagesReceived = 0;
    protected long rowsReceived = 0;

    protected CompleteInputCollectorOperator() {
        this.finished = false;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        inputPages.add(page);
        pagesReceived++;
        rowsReceived += page.getPositionCount();
    }

    @Override
    public void finish() {
        if (finished == false) {
            finished = true;
            onFinished();
        }
    }

    @Override
    public boolean isFinished() {
        return finished && isOperatorFinished();
    }

    @Override
    public Page getOutput() {
        if (finished == false || isOperatorFinished()) {
            return null;
        }

        return onGetOutput();
    }

    @Override
    public void close() {
        Releasables.close(inputPages);
        onClose();
    }

    /**
     * Called when he base finish() call is completed
     */
    protected abstract void onFinished();

    /**
     * Adds any additional checks to ensure the subclass is finished
     * @return true if the subclass is finished
     */
    protected abstract boolean isOperatorFinished();

    /**
     * Implementation to retrieve the output pages. Will only run if `isFinished()` is true
     * @return the output page (or null if not ready or none)
     */
    protected abstract Page onGetOutput();

    /**
     * Additional implementation for closing out the subclass
     */
    protected abstract void onClose();
}
