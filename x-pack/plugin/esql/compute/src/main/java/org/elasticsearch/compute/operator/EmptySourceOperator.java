/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;

/**
 * An empty source operator, which is already finished and never emits any output.
 */
public final class EmptySourceOperator extends SourceOperator {

    public static class Factory implements SourceOperatorFactory {
        @Override
        public String describe() {
            return "EmptySourceOperator[]";
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new EmptySourceOperator();
        }
    }

    @Override
    public void finish() {

    }

    @Override
    public boolean isFinished() {
        return true;
    }

    @Override
    public Page getOutput() {
        return null;
    }

    @Override
    public void close() {

    }
}
