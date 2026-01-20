/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

public class MMRExecSerializationTests extends AbstractPhysicalPlanSerializationTests<MMRExec> {

    @Override
    protected MMRExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        Double randomLambda = randomBoolean() ? randomDouble() : null;
        return new MMRExec(
            source,
            child,
            randomField(),
            randomLimit(),
            randomQueryVector(),
            getOptionsFromLambda(randomLambda),
            randomLambda
        );
    }

    private Attribute randomField() {
        return FieldAttributeTests.createFieldAttribute(3, randomBoolean());
    }

    private Expression randomLimit() {
        return new Literal(Source.EMPTY, randomIntBetween(5, 20), DataType.INTEGER);
    }

    private Expression randomQueryVector() {
        if (randomBoolean()) {
            return null;
        }

        return FieldAttributeTests.createFieldAttribute(3, randomBoolean());
    }

    private MapExpression getOptionsFromLambda(Double lambdaValue) {
        if (lambdaValue == null) {
            return null;
        }

        return new MapExpression(
            Source.EMPTY,
            List.of(
                new Literal(Source.EMPTY, BytesRefs.toBytesRef("lambda"), KEYWORD),
                new Literal(Source.EMPTY, lambdaValue, DataType.DOUBLE)
            )
        );
    }

    @Override
    protected MMRExec mutateInstance(MMRExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Attribute field = instance.diversifyField();
        Expression limit = instance.limit();
        Expression queryVector = instance.queryVector();
        Expression options = instance.options();
        Double lambdaValue = instance.lambdaValue();

        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> field = randomValueOtherThan(field, this::randomField);
            case 2 -> limit = randomValueOtherThan(limit, this::randomLimit);
            case 3 -> queryVector = randomValueOtherThan(queryVector, this::randomQueryVector);
            case 4 -> {
                lambdaValue = randomValueOtherThan(lambdaValue, () -> randomBoolean() ? randomDouble() : null);
                options = getOptionsFromLambda(lambdaValue);
            }
        }

        return new MMRExec(instance.source(), child, field, limit, queryVector, options, lambdaValue);
    }
}
