/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.score;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DecaySerializationTests extends AbstractExpressionSerializationTests<Decay> {
    @Override
    protected Decay createTestInstance() {
        Source source = randomSource();
        Expression value = randomChild();
        Expression origin = randomChild();
        Expression scale = randomChild();
        Expression options = randomBoolean() ? null : randomOptions();
        return new Decay(source, value, origin, scale, options);
    }

    @Override
    protected Decay mutateInstance(Decay instance) throws IOException {
        Source source = instance.source();
        Expression value = instance.value();
        Expression origin = instance.origin();
        Expression scale = instance.scale();
        Expression options = instance.options();
        switch (between(0, 3)) {
            case 0 -> value = randomValueOtherThan(value, AbstractExpressionSerializationTests::randomChild);
            case 1 -> origin = randomValueOtherThan(origin, AbstractExpressionSerializationTests::randomChild);
            case 2 -> scale = randomValueOtherThan(scale, AbstractExpressionSerializationTests::randomChild);
            case 3 -> options = randomValueOtherThan(options, () -> randomBoolean() ? null : randomOptions());
        }
        return new Decay(source, value, origin, scale, options);
    }

    private MapExpression randomOptions() {
        List<Expression> entries = new ArrayList<>();
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "offset"));
            entries.add(new Literal(Source.EMPTY, randomDoubleBetween(0, 100, true), DataType.DOUBLE));
        }
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "decay"));
            entries.add(new Literal(Source.EMPTY, randomDoubleBetween(0.01, 0.99, true), DataType.DOUBLE));
        }
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "type"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("linear", "exp", "gauss")));
        }
        return new MapExpression(Source.EMPTY, entries);
    }
}
