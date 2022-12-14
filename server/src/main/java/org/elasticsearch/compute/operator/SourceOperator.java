/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Page;

/**
 * A source operator - produces output, accepts no input.
 */
public abstract class SourceOperator implements Operator {

    /**
     * A source operator needs no input - unconditionally returns false.
     * @return false
     */
    public final boolean needsInput() {
        return false;
    }

    /**
     * A source operator does not accept input - unconditionally throws UnsupportedOperationException.
     * @param page a page
     */
    @Override
    public final void addInput(Page page) {
        throw new UnsupportedOperationException();
    }

    /**
     * A factory for creating source operators.
     */
    public interface SourceOperatorFactory extends Describable {
        /** Creates a new source operator. */
        SourceOperator get();
    }
}
