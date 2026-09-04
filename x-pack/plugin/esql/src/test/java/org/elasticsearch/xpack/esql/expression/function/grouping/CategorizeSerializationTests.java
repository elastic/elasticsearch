/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

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

public class CategorizeSerializationTests extends AbstractExpressionSerializationTests<Categorize> {
    @Override
    protected Categorize createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression options = randomBoolean() ? null : randomOptions();
        return new Categorize(source, field, options);
    }

    @Override
    protected Categorize mutateInstance(Categorize instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression options = instance.children().size() > 1 ? instance.children().get(1) : null;
        switch (between(0, 1)) {
            case 0 -> field = randomValueOtherThan(field, AbstractUnaryScalarSerializationTests::randomChild);
            case 1 -> options = randomValueOtherThan(options, () -> randomBoolean() ? null : randomOptions());
        }
        return new Categorize(source, field, options);
    }

    private MapExpression randomOptions() {
        List<Expression> entries = new ArrayList<>();
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "analyzer"));
            entries.add(Literal.keyword(Source.EMPTY, "standard"));
        }
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "output_format"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("regex", "tokens")));
        }
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "similarity_threshold"));
            entries.add(new Literal(Source.EMPTY, randomIntBetween(1, 100), DataType.INTEGER));
        }
        return new MapExpression(Source.EMPTY, entries);
    }
}
