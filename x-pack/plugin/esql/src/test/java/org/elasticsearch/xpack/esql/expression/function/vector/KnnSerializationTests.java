/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KnnSerializationTests extends AbstractExpressionSerializationTests<Knn> {
    @Override
    protected Knn createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression query = randomChild();
        Expression options = randomOptions();
        Integer implicitK = randomIntBetween(10, 100);
        List<Expression> filterExpressions = randomList(0, 3, AbstractExpressionSerializationTests::randomChild);
        return new Knn(source, field, query, options, implicitK, null, filterExpressions);
    }

    @Override
    protected Knn mutateInstance(Knn instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression query = instance.query();
        Expression options = instance.options();
        Integer implicitK = instance.implicitK();
        List<Expression> filterExpressions = instance.filterExpressions();
        switch (between(0, 4)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> query = randomValueOtherThan(query, AbstractExpressionSerializationTests::randomChild);
            case 2 -> options = randomValueOtherThan(options, () -> randomBoolean() ? null : randomOptions());
            case 3 -> filterExpressions = randomValueOtherThan(
                filterExpressions,
                () -> randomList(0, 3, AbstractExpressionSerializationTests::randomChild)
            );
            case 4 -> implicitK = randomValueOtherThan(instance.implicitK(), () -> randomIntBetween(10, 100));
        }
        return new Knn(source, field, query, options, implicitK, null, filterExpressions);
    }

    private Expression randomOptions() {
        if (randomBoolean()) {
            return null;
        }

        List<Expression> entries = new ArrayList<>();
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "boost"));
            entries.add(Literal.fromDouble(Source.EMPTY, randomDoubleBetween(0, 10, true)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "k"));
            entries.add(Literal.integer(Source.EMPTY, randomIntBetween(1, 100)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "min_candidates"));
            entries.add(Literal.integer(Source.EMPTY, randomIntBetween(1, 1000)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "rescore_oversample"));
            entries.add(Literal.fromDouble(Source.EMPTY, randomDoubleBetween(1, 10, true)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "similarity"));
            entries.add(Literal.fromDouble(Source.EMPTY, randomDoubleBetween(0, 1, true)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "visit_percentage"));
            entries.add(Literal.fromDouble(Source.EMPTY, randomDoubleBetween(0, 1, true)));
        }

        return new MapExpression(Source.EMPTY, entries);
    }
}
