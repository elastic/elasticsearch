/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class MvInRangeSerializationTests extends AbstractExpressionSerializationTests<MvInRange> {
    @Override
    protected MvInRange createTestInstance() {
        Source source = randomSource();
        return new MvInRange(source, randomChild(), randomChild(), randomChild(), randomBoolean() ? null : randomOptions());
    }

    @Override
    protected MvInRange mutateInstance(MvInRange instance) throws IOException {
        Expression field = instance.field();
        Expression lower = instance.lower();
        Expression upper = instance.upper();
        Expression options = instance.options();
        switch (between(0, 3)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> lower = randomValueOtherThan(lower, AbstractExpressionSerializationTests::randomChild);
            case 2 -> upper = randomValueOtherThan(upper, AbstractExpressionSerializationTests::randomChild);
            default -> options = randomValueOtherThan(options, () -> randomBoolean() ? null : randomOptions());
        }
        return new MvInRange(instance.source(), field, lower, upper, options);
    }

    private static Expression randomOptions() {
        return new MapExpression(
            Source.EMPTY,
            List.of(
                Literal.keyword(Source.EMPTY, "include_lower"),
                new Literal(Source.EMPTY, randomBoolean(), DataType.BOOLEAN),
                Literal.keyword(Source.EMPTY, "include_upper"),
                new Literal(Source.EMPTY, randomBoolean(), DataType.BOOLEAN)
            )
        );
    }
}
