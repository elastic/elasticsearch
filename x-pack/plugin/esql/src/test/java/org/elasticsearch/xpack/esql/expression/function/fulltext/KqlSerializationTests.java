/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KqlSerializationTests extends AbstractExpressionSerializationTests<Kql> {
    @Override
    protected Kql createTestInstance() {
        Source source = randomSource();
        Expression query = randomChild();
        Expression options = randomOptions();
        return new Kql(source, query, options, configuration());
    }

    @Override
    protected Kql mutateInstance(Kql instance) throws IOException {
        Source source = instance.source();
        Expression query = instance.query();
        Expression options = instance.options();
        switch (between(0, 1)) {
            case 0 -> query = randomValueOtherThan(query, AbstractExpressionSerializationTests::randomChild);
            case 1 -> options = randomValueOtherThan(options, this::randomOptions);
        }
        return new Kql(source, query, options, configuration());
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
            entries.add(Literal.keyword(Source.EMPTY, "case_insensitive"));
            entries.add(Literal.fromBoolean(Source.EMPTY, randomBoolean()));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "default_field"));
            entries.add(Literal.keyword(Source.EMPTY, randomAlphaOfLength(5)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "time_zone"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("UTC", "America/New_York", "Europe/Paris")));
        }

        return new MapExpression(Source.EMPTY, entries);
    }
}
