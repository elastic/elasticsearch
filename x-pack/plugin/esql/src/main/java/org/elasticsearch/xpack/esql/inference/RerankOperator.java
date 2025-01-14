/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;

public class RerankOperator extends AsyncOperator {
    public record Factory(int maxOutstandingRequests) implements OperatorFactory {
        @Override
        public RerankOperator get(DriverContext driverContext) {
            return new RerankOperator(driverContext, maxOutstandingRequests);
        }

        @Override
        public String describe() {
            return "RerankOperator[maxOutstandingRequests = " + maxOutstandingRequests + "]";
        }
    }

    public RerankOperator(DriverContext driverContext, int maxOutstandingRequests) {
        super(driverContext, maxOutstandingRequests);
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        listener.onResponse(inputPage);
    }

    @Override
    protected void doClose() {

    }
}
