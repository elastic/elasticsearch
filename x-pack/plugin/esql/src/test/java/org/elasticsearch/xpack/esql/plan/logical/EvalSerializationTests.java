/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AliasTests;

import java.io.IOException;
import java.util.List;

public class EvalSerializationTests extends AbstractLogicalPlanSerializationTests<Eval> {
    @Override
    protected Eval createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        List<Alias> fields = randomList(0, 10, AliasTests::randomAlias);
        return new Eval(source, child, fields);
    }

    @Override
    protected Eval mutateInstance(Eval instance) throws IOException {
        LogicalPlan child = instance.child();
        List<Alias> fields = instance.fields();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, () -> randomChild(0));
        } else {
            fields = randomValueOtherThan(fields, () -> randomList(0, 10, AliasTests::randomAlias));
        }
        return new Eval(instance.source(), child, fields);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
