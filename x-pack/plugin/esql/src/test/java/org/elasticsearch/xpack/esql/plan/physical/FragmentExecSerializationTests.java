/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequestTests;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class FragmentExecSerializationTests extends AbstractPhysicalPlanSerializationTests<FragmentExec> {
    public static FragmentExec randomFragmentExec(int depth) {
        Source source = randomSource();
        LogicalPlan fragment = AbstractLogicalPlanSerializationTests.randomChild(depth);
        QueryBuilder esFilter = EsqlQueryRequestTests.randomQueryBuilder();
        int estimatedRowSize = between(0, Integer.MAX_VALUE);
        return new FragmentExec(source, fragment, esFilter, estimatedRowSize);
    }

    @Override
    protected FragmentExec createTestInstance() {
        return randomFragmentExec(0);
    }

    @Override
    protected FragmentExec mutateInstance(FragmentExec instance) throws IOException {
        LogicalPlan fragment = instance.fragment();
        QueryBuilder esFilter = instance.esFilter();
        int estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 2)) {
            case 0 -> fragment = randomValueOtherThan(fragment, () -> AbstractLogicalPlanSerializationTests.randomChild(0));
            case 1 -> esFilter = randomValueOtherThan(esFilter, EsqlQueryRequestTests::randomQueryBuilder);
            case 2 -> estimatedRowSize = randomValueOtherThan(estimatedRowSize, () -> between(0, Integer.MAX_VALUE));
            default -> throw new UnsupportedEncodingException();
        }
        return new FragmentExec(instance.source(), fragment, esFilter, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
