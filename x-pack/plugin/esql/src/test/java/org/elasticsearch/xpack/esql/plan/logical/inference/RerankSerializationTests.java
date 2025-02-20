/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AliasTests;
import org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;

public class RerankSerializationTests extends AbstractLogicalPlanSerializationTests<Rerank> {
    @Override
    protected Rerank createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        return new Rerank(source, child, randomIdentifier(), randomIdentifier(), randomFields());
    }

    @Override
    protected Rerank mutateInstance(Rerank instance) throws IOException {
        LogicalPlan child = instance.child();
        String inferenceId = instance.inferenceId();
        String queryText = instance.queryText();
        List<Alias> fields = instance.rerankFields();

        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, RerankSerializationTests::randomIdentifier);
            case 2 -> queryText = randomValueOtherThan(queryText, RerankSerializationTests::randomIdentifier);
            case 3 -> fields = randomValueOtherThan(fields, this::randomFields);
        }
        return new Rerank(instance.source(), child, inferenceId, queryText, fields);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    private List<Alias> randomFields() {
        return randomList(0, 10, AliasTests::randomAlias);
    }
}
