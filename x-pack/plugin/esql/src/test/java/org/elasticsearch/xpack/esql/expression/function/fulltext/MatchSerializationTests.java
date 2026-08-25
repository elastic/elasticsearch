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

public class MatchSerializationTests extends AbstractExpressionSerializationTests<Match> {
    @Override
    protected Match createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression query = randomChild();
        Expression options = randomOptions();
        return new Match(source, field, query, options);
    }

    @Override
    protected Match mutateInstance(Match instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression query = instance.query();
        Expression options = instance.options();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> query = randomValueOtherThan(query, AbstractExpressionSerializationTests::randomChild);
            case 2 -> options = randomValueOtherThan(options, this::randomOptions);
        }

        return new Match(source, field, query, options);
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
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("0.5", "1.0", "2.0")));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "fuzziness"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("AUTO", "1", "2")));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "fuzzy_rewrite"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("constant_score", "constant_score_blended", "constant_score_boolean")));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "fuzzy_transpositions"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("true", "false")));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "generate_synonyms_phrase_query"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("true", "false")));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "lenient"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("true", "false")));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "max_expansions"));
            entries.add(Literal.integer(Source.EMPTY, randomIntBetween(1, 50)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "minimum_should_match"));
            entries.add(Literal.keyword(Source.EMPTY, String.valueOf(randomIntBetween(1, 3))));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "operator"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("and", "or")));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "prefix_length"));
            entries.add(Literal.integer(Source.EMPTY, randomIntBetween(0, 5)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "zero_terms_query"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("none", "all")));
        }

        return new MapExpression(Source.EMPTY, entries);
    }
}
