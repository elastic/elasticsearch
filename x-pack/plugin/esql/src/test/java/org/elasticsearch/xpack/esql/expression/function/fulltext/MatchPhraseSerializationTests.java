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

public class MatchPhraseSerializationTests extends AbstractExpressionSerializationTests<MatchPhrase> {
    @Override
    protected MatchPhrase createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression query = randomChild();
        Expression options = randomOptions();
        return new MatchPhrase(source, field, query, options);
    }

    @Override
    protected MatchPhrase mutateInstance(MatchPhrase instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression query = instance.query();
        Expression options = instance.options();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> query = randomValueOtherThan(query, AbstractExpressionSerializationTests::randomChild);
            case 2 -> options = randomValueOtherThan(options, this::randomOptions);
        }
        return new MatchPhrase(source, field, query, options);
    }

    private Expression randomOptions() {
        if (randomBoolean()) {
            return null;
        }

        List<Expression> entries = new ArrayList<>();
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "analyzer"));
            entries.add(Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "boost"));
            entries.add(Literal.fromDouble(Source.EMPTY, randomDoubleBetween(0, 10, true)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "slop"));
            entries.add(Literal.integer(Source.EMPTY, randomIntBetween(0, 10)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "zero_terms_query"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("none", "all")));
        }

        return new MapExpression(Source.EMPTY, entries);
    }
}
