/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChickenSerializationTests extends AbstractExpressionSerializationTests<Chicken> {
    @Override
    protected Chicken createTestInstance() {
        Source source = randomSource();
        Expression message = randomChild();
        Expression options = randomBoolean() ? null : randomOptions();
        return new Chicken(source, message, options);
    }

    @Override
    protected Chicken mutateInstance(Chicken instance) throws IOException {
        Source source = instance.source();
        Expression message = instance.children().get(0);
        Expression options = instance.children().size() > 1 ? instance.children().get(1) : null;
        switch (between(0, 1)) {
            case 0 -> message = randomValueOtherThan(message, AbstractUnaryScalarSerializationTests::randomChild);
            case 1 -> options = randomValueOtherThan(options, () -> randomBoolean() ? null : randomOptions());
        }
        return new Chicken(source, message, options);
    }

    private MapExpression randomOptions() {
        List<Expression> entries = new ArrayList<>();
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "style"));
            entries.add(
                Literal.keyword(
                    Source.EMPTY,
                    randomFrom(
                        "ordinary",
                        "early_state",
                        "laying",
                        "thinks_its_a_duck",
                        "smoking_a_pipe",
                        "soup",
                        "racing",
                        "stoned",
                        "realistic",
                        "whistling"
                    )
                )
            );
        }
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "width"));
            entries.add(new Literal(Source.EMPTY, randomIntBetween(1, 76), DataType.INTEGER));
        }
        return new MapExpression(Source.EMPTY, entries);
    }
}
