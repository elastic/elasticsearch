/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;

public class MMRSerializationTests extends AbstractLogicalPlanSerializationTests<MMR> {
    @Override
    protected MMR createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        return new MMR(source, child, randomField(), randomLimit(), null, null);
    }

    private Attribute randomField() {
        return FieldAttributeTests.createFieldAttribute(3, randomBoolean());
    }

    private Expression randomLimit() {
        return new Literal(Source.EMPTY, randomIntBetween(5, 20), DataType.INTEGER);
    }

    @Override
    protected MMR mutateInstance(MMR instance) throws IOException {
        LogicalPlan child = instance.child();
        Attribute field = instance.diversifyField();
        Expression limit = instance.limit();
        Expression queryVector = instance.queryVector();
        Expression lambda = instance.lambdaValue();

        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> field = randomValueOtherThan(field, this::randomField);
            case 2 -> limit = randomValueOtherThan(limit, this::randomLimit);
        }

        return new MMR(instance.source(), child, field, limit, queryVector, lambda);
    }
}
