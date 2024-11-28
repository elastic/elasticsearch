/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.Build;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.kql.KqlPlugin;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class KqlQueryBuilderTests extends AbstractQueryTestCase<KqlQueryBuilder> {
    @BeforeClass
    protected static void ensureSnapshotBuild() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(KqlPlugin.class);
    }

    @Override
    protected KqlQueryBuilder doCreateTestQueryBuilder() {
        KqlQueryBuilder kqlQueryBuilder = new KqlQueryBuilder(generateRandomKqlQuery());

        if (randomBoolean()) {
            kqlQueryBuilder.caseInsensitive(randomBoolean());
        }

        if (randomBoolean()) {
            kqlQueryBuilder.timeZone(randomTimeZone().getID());
        }

        if (randomBoolean()) {
            kqlQueryBuilder.defaultField(randomFrom("*", "mapped_*", KEYWORD_FIELD_NAME, TEXT_FIELD_NAME));
        }

        return kqlQueryBuilder;
    }

    @Override
    public KqlQueryBuilder mutateInstance(KqlQueryBuilder instance) throws IOException {
        if (randomBoolean()) {
            // Change name or boost.
            return super.mutateInstance(instance);
        }

        KqlQueryBuilder kqlQueryBuilder = new KqlQueryBuilder(randomValueOtherThan(instance.queryString(), this::generateRandomKqlQuery))
            .caseInsensitive(instance.caseInsensitive())
            .timeZone(instance.timeZone() != null ? instance.timeZone().getId() : null)
            .defaultField(instance.defaultField());

        if (kqlQueryBuilder.queryString().equals(instance.queryString()) == false) {
            return kqlQueryBuilder;
        }

        switch (randomInt() % 3) {
            case 0 -> {
                kqlQueryBuilder.caseInsensitive(instance.caseInsensitive() == false);
            }
            case 1 -> {
                if (randomBoolean() && instance.defaultField() != null) {
                    kqlQueryBuilder.defaultField(null);
                } else {
                    kqlQueryBuilder.defaultField(
                        randomValueOtherThan(
                            instance.defaultField(),
                            () -> randomFrom("*", "mapped_*", KEYWORD_FIELD_NAME, TEXT_FIELD_NAME)
                        )
                    );
                }
            }
            default -> {
                if (randomBoolean() && instance.timeZone() != null) {
                    kqlQueryBuilder.timeZone(null);
                } else if (instance.timeZone() != null) {
                    kqlQueryBuilder.timeZone(randomValueOtherThan(instance.timeZone().getId(), () -> randomTimeZone().getID()));
                } else {
                    kqlQueryBuilder.timeZone(randomTimeZone().getID());
                }
            }
        }
        ;

        return kqlQueryBuilder;
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

    public void testCaseInsensitiveWildcardQuery() throws IOException {
        QueryRewriteContext queryRewriteContext = createQueryRewriteContext();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (boolean caseInsensitive : List.of(true, false)) {
            KqlQueryBuilder kqlQuery = new KqlQueryBuilder(KEYWORD_FIELD_NAME + ": foo*");
            // Check case case_insensitive is true by default
            assertThat(kqlQuery.caseInsensitive(), equalTo(true));

            kqlQuery.caseInsensitive(caseInsensitive);

            ;
            assertThat(
                asInstanceOf(WildcardQueryBuilder.class, rewriteQuery(kqlQuery, queryRewriteContext, searchExecutionContext))
                    .caseInsensitive(),
                equalTo(caseInsensitive)
            );
        }
    }

    public void testCaseInsensitiveTermQuery() throws IOException {
        QueryRewriteContext queryRewriteContext = createQueryRewriteContext();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (boolean caseInsensitive : List.of(true, false)) {
            KqlQueryBuilder kqlQuery = new KqlQueryBuilder(KEYWORD_FIELD_NAME + ": foo");
            // Check case case_insensitive is true by default
            assertThat(kqlQuery.caseInsensitive(), equalTo(true));

            kqlQuery.caseInsensitive(caseInsensitive);

            assertThat(
                asInstanceOf(TermQueryBuilder.class, rewriteQuery(kqlQuery, queryRewriteContext, searchExecutionContext)).caseInsensitive(),
                equalTo(caseInsensitive)
            );
        }
    }

    public void testTimeZone() throws IOException {
        QueryRewriteContext queryRewriteContext = createQueryRewriteContext();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        String timeZone = randomTimeZone().getID();

        for (String operator : List.of(":", "<", "<=", ">", ">=")) {
            KqlQueryBuilder kqlQuery = new KqlQueryBuilder(Strings.format("%s %s %s", DATE_FIELD_NAME, operator, "2018-03-28"));
            assertThat(kqlQuery.timeZone(), nullValue()); // timeZone is not set by default.
            kqlQuery.timeZone(timeZone);

            assertThat(
                asInstanceOf(RangeQueryBuilder.class, rewriteQuery(kqlQuery, queryRewriteContext, searchExecutionContext)).timeZone(),
                equalTo(timeZone)
            );
        }
    }

    public void testDefaultFieldWildcardQuery() throws IOException {
        QueryRewriteContext queryRewriteContext = createQueryRewriteContext();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        KqlQueryBuilder kqlQuery = new KqlQueryBuilder(Strings.format("foo*"));
        assertThat(kqlQuery.defaultField(), nullValue()); // default_field is not set by default.

        kqlQuery.defaultField(TEXT_FIELD_NAME);

        assertThat(
            asInstanceOf(QueryStringQueryBuilder.class, rewriteQuery(kqlQuery, queryRewriteContext, searchExecutionContext)).defaultField(),
            equalTo(TEXT_FIELD_NAME)
        );
    }

    public void testDefaultFieldMatchQuery() throws IOException {

        QueryRewriteContext queryRewriteContext = createQueryRewriteContext();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        {
            // Using a specific field name
            KqlQueryBuilder kqlQuery = new KqlQueryBuilder(Strings.format("foo"));
            assertThat(kqlQuery.defaultField(), nullValue()); // default_field is not set by default.

            kqlQuery.defaultField(TEXT_FIELD_NAME);
            MultiMatchQueryBuilder rewritenQuery = asInstanceOf(
                MultiMatchQueryBuilder.class,
                rewriteQuery(kqlQuery, queryRewriteContext, searchExecutionContext)
            );
            assertThat(rewritenQuery.fields().keySet(), contains(TEXT_FIELD_NAME));
        }

        {
            // Using a pattern for as the field name
            KqlQueryBuilder kqlQuery = new KqlQueryBuilder(Strings.format("foo"));
            assertThat(kqlQuery.defaultField(), nullValue()); // default_field is not set by default.

            kqlQuery.defaultField("mapped_object.*");
            MultiMatchQueryBuilder rewritenQuery = asInstanceOf(
                MultiMatchQueryBuilder.class,
                rewriteQuery(kqlQuery, queryRewriteContext, searchExecutionContext)
            );
            assertThat(rewritenQuery.fields().keySet(), contains("mapped_object.mapped_date", "mapped_object.mapped_int"));
        }
    }

    public void testQueryNameIsPreserved() throws IOException {
        QueryRewriteContext queryRewriteContext = createQueryRewriteContext();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        KqlQueryBuilder kqlQuery = new KqlQueryBuilder(generateRandomKqlQuery()).queryName(randomIdentifier());
        QueryBuilder rewrittenQuery = rewriteQuery(kqlQuery, queryRewriteContext, searchExecutionContext);
        assertThat(rewrittenQuery.queryName(), equalTo(kqlQuery.queryName()));
    }

    public void testQueryBoostIsPreserved() throws IOException {
        QueryRewriteContext queryRewriteContext = createQueryRewriteContext();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        KqlQueryBuilder kqlQuery = new KqlQueryBuilder(generateRandomKqlQuery()).boost(randomFloatBetween(0, Float.MAX_VALUE, true));
        QueryBuilder rewrittenQuery = rewriteQuery(kqlQuery, queryRewriteContext, searchExecutionContext);
        assertThat(rewrittenQuery.boost(), equalTo(kqlQuery.boost()));
    }
}
