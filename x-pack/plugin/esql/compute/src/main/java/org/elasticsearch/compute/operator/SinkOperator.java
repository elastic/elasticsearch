/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Page;

/**
 * A sink operator - accepts input, produces no output.
 */
public abstract class SinkOperator implements Operator {

    /**
     * A sink operator produces no output - unconditionally throws UnsupportedOperationException
     */
    @Override
    public final Page getOutput() {
        throw new UnsupportedOperationException();
    }

    protected abstract void doAddInput(Page page);

    @Override
    public final void addInput(Page page) {
        // We need to change the ownership of the blocks of the input page before passing them to another driver.
        page.allowPassingToDifferentDriver();
        doAddInput(page);
    }

    /**
     * A factory for creating sink operators.
     */
    public interface SinkOperatorFactory extends OperatorFactory, Describable {
        /** Creates a new sink operator. */
        SinkOperator get(DriverContext driverContext);
    }

}
