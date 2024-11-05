/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.kql.KqlPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KqlQueryBuilderTests extends AbstractQueryTestCase<KqlQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(KqlPlugin.class);
    }

    @Override
    protected KqlQueryBuilder doCreateTestQueryBuilder() {
        return new KqlQueryBuilder(generateRandomKqlQuery());
    }

    @Override
    public KqlQueryBuilder mutateInstance(KqlQueryBuilder instance) throws IOException {
        if (randomBoolean()) {
            // Change name or boost.
            return super.mutateInstance(instance);
        }

        return new KqlQueryBuilder(randomValueOtherThan(instance.queryString(), this::generateRandomKqlQuery));
    }

    @Override
    protected void doAssertLuceneQuery(KqlQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        // We're not validating the query content here because it would be too complex.
        // Instead, we use ad-hoc parser tests with a predictable output.
    }

    private String generateRandomKqlQuery() {
        return Stream.generate(() -> {
            Stream<String> terms = Stream.generate(
                () -> randomValueOtherThanMany(s -> s.toLowerCase(Locale.ROOT).contains("now"), () -> randomAlphaOfLengthBetween(4, 10))
            ).limit(randomIntBetween(1, 5));

            String subQuery = terms.collect(Collectors.joining(" "));

            if (randomBoolean() && subQuery.isEmpty() == false) {
                String operator = randomFrom(":", "<", "<=", ">", ">=");
                String fieldName = randomFrom(KEYWORD_FIELD_NAME, TEXT_FIELD_NAME);
                if (operator.equals(":")) {
                    subQuery = switch (randomFrom(0, 2)) {
                        case 0 -> subQuery;
                        case 1 -> '(' + subQuery + ')';
                        default -> '"' + subQuery + '"';
                    };
                } else {
                    fieldName = randomFrom(KEYWORD_FIELD_NAME, TEXT_FIELD_NAME, DOUBLE_FIELD_NAME, INT_FIELD_NAME);
                    if (List.of(DOUBLE_FIELD_NAME, INT_FIELD_NAME).contains(fieldName)) {
                        subQuery = String.valueOf(randomDouble());
                    }
                    subQuery = randomBoolean() ? '"' + subQuery + '"' : subQuery;
                }

                subQuery = fieldName + operator + subQuery;
            }

            if (randomBoolean() && subQuery.isEmpty() == false) {
                subQuery = '(' + subQuery + ')';
            }

            if (randomBoolean()) {
                subQuery = "NOT " + subQuery;
            }

            if (randomBoolean() && subQuery.isEmpty() == false) {
                subQuery = '(' + subQuery + ')';
            }

            return subQuery;
        }).limit(randomIntBetween(0, 5)).collect(Collectors.joining(randomFrom(" OR ", " AND ")));
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        KqlQueryBuilder queryBuilder = createTestQueryBuilder();
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertThat(e.getMessage(), Matchers.containsString("The query should have been rewritten"));
    }
}
