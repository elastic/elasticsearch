/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.QueryBuilderResolver;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.function.BiConsumer;

public class MockQueryBuilderResolver extends QueryBuilderResolver {
    public MockQueryBuilderResolver() {
        super(null, null, null, null);
    }

    @Override
    public void resolveQueryBuilders(
        LogicalPlan plan,
        ActionListener<Result> listener,
        BiConsumer<LogicalPlan, ActionListener<Result>> callback
    ) {
        callback.accept(plan, listener);
    }
}
