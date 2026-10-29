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

public class QueryStringSerializationTests extends AbstractExpressionSerializationTests<QueryString> {
    @Override
    protected QueryString createTestInstance() {
        Source source = randomSource();
        Expression query = randomChild();
        Expression options = randomOptions();
        return new QueryString(source, query, options, configuration());
    }

    @Override
    protected QueryString mutateInstance(QueryString instance) throws IOException {
        Source source = instance.source();
        Expression query = instance.query();
        Expression options = instance.options();
        switch (between(0, 1)) {
            case 0 -> query = randomValueOtherThan(query, AbstractExpressionSerializationTests::randomChild);
            case 1 -> options = randomValueOtherThan(options, this::randomOptions);
        }
        return new QueryString(source, query, options, configuration());
    }

    private Expression randomOptions() {
        if (randomBoolean()) {
            return null;
        }

        List<Expression> entries = new ArrayList<>();
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "allow_leading_wildcard"));
            entries.add(Literal.fromBoolean(Source.EMPTY, randomBoolean()));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "analyze_wildcard"));
            entries.add(Literal.fromBoolean(Source.EMPTY, randomBoolean()));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "analyzer"));
            entries.add(Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "boost"));
            entries.add(Literal.fromDouble(Source.EMPTY, randomDoubleBetween(0, 10, true)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "default_field"));
            entries.add(Literal.keyword(Source.EMPTY, randomAlphaOfLength(5)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "default_operator"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("and", "or")));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "enable_position_increments"));
            entries.add(Literal.fromBoolean(Source.EMPTY, randomBoolean()));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "fuzziness"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("AUTO", "1", "2")));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "fuzzy_max_expansions"));
            entries.add(Literal.integer(Source.EMPTY, randomIntBetween(1, 50)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "fuzzy_prefix_length"));
            entries.add(Literal.integer(Source.EMPTY, randomIntBetween(0, 5)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "fuzzy_transpositions"));
            entries.add(Literal.fromBoolean(Source.EMPTY, randomBoolean()));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "generate_synonyms_phrase_query"));
            entries.add(Literal.fromBoolean(Source.EMPTY, randomBoolean()));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "lenient"));
            entries.add(Literal.fromBoolean(Source.EMPTY, randomBoolean()));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "max_determinized_states"));
            entries.add(Literal.integer(Source.EMPTY, randomIntBetween(100, 10000)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "minimum_should_match"));
            entries.add(Literal.keyword(Source.EMPTY, String.valueOf(randomIntBetween(1, 3))));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "phrase_slop"));
            entries.add(Literal.integer(Source.EMPTY, randomIntBetween(0, 5)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "quote_analyzer"));
            entries.add(Literal.keyword(Source.EMPTY, randomAlphaOfLength(10)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "quote_field_suffix"));
            entries.add(Literal.keyword(Source.EMPTY, randomAlphaOfLength(5)));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "rewrite"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("constant_score", "scoring_boolean", "top_terms_10")));
        }

        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "time_zone"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("UTC", "America/New_York", "Europe/Paris")));
        }

        return new MapExpression(Source.EMPTY, entries);
    }
}
