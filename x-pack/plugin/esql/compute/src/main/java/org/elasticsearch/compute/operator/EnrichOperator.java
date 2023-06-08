/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;

public class EnrichOperator extends AbstractPageMappingOperator {
    public record EnrichOperatorFactory() implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new EnrichOperator();
        }

        @Override
        public String describe() {
            return "EnrichOperator[]";
        }
    }

    @Override
    protected Page process(Page page) {
        // TODO
        throw new UnsupportedOperationException("Implement enrich operator!");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
