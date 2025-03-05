/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;

import java.io.IOException;
import java.util.List;

public class EvalExecSerializationTests extends AbstractPhysicalPlanSerializationTests<EvalExec> {
    public static EvalExec randomEvalExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        List<Alias> fields = randomFields();
        return new EvalExec(source, child, fields);
    }

    public static List<Alias> randomFields() {
        return randomList(1, 10, EvalExecSerializationTests::randomField);
    }

    public static Alias randomField() {
        Expression child = new Add(
            randomSource(),
            FieldAttributeTests.createFieldAttribute(0, true),
            FieldAttributeTests.createFieldAttribute(0, true)
        );
        return new Alias(randomSource(), randomAlphaOfLength(5), child);
    }

    @Override
    protected EvalExec createTestInstance() {
        return randomEvalExec(0);
    }

    @Override
    protected EvalExec mutateInstance(EvalExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        List<Alias> fields = instance.fields();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, () -> randomChild(0));
        } else {
            fields = randomValueOtherThan(fields, EvalExecSerializationTests::randomFields);
        }
        return new EvalExec(instance.source(), child, fields);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
