/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

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

    /**
     * A factory for creating sink operators.
     */
    public interface SinkOperatorFactory extends OperatorFactory {

        /** Creates a new sink operator. */
        SinkOperator get();
    }

}
