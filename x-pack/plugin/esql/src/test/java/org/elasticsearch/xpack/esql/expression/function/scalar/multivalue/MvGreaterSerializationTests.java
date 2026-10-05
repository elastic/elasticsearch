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

public class MvGreaterSerializationTests extends AbstractExpressionSerializationTests<MvGreater> {
    @Override
    protected MvGreater createTestInstance() {
        Source source = randomSource();
        return new MvGreater(source, randomChild(), randomChild(), randomBoolean() ? null : randomOptions());
    }

    @Override
    protected MvGreater mutateInstance(MvGreater instance) throws IOException {
        Expression field = instance.field();
        Expression bound = instance.bound();
        Expression options = instance.options();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> bound = randomValueOtherThan(bound, AbstractExpressionSerializationTests::randomChild);
            default -> options = randomValueOtherThan(options, () -> randomBoolean() ? null : randomOptions());
        }
        return new MvGreater(instance.source(), field, bound, options);
    }

    private static Expression randomOptions() {
        return new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "include_bound"), new Literal(Source.EMPTY, randomBoolean(), DataType.BOOLEAN))
        );
    }
}
