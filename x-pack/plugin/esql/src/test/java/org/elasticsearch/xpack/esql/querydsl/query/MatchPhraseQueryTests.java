/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.ZeroTermsQueryOption;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;

public class MatchPhraseQueryTests extends ESTestCase {
    static MatchPhraseQuery randomMatchPhraseQuery() {
        return new MatchPhraseQuery(SourceTests.randomSource(), randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomMatchPhraseQuery(), MatchPhraseQueryTests::copy, MatchPhraseQueryTests::mutate);
    }

    private static MatchPhraseQuery copy(MatchPhraseQuery query) {
        return new MatchPhraseQuery(query.source(), query.name(), query.text(), query.options());
    }

    private static MatchPhraseQuery mutate(MatchPhraseQuery query) {
        List<Function<MatchPhraseQuery, MatchPhraseQuery>> options = Arrays.asList(
            q -> new MatchPhraseQuery(SourceTests.mutate(q.source()), q.name(), q.text(), q.options()),
            q -> new MatchPhraseQuery(q.source(), randomValueOtherThan(q.name(), () -> randomAlphaOfLength(5)), q.text(), q.options()),
            q -> new MatchPhraseQuery(q.source(), q.name(), randomValueOtherThan(q.text(), () -> randomAlphaOfLength(5)), q.options())
        );
        return randomFrom(options).apply(query);
    }

    public void testQueryBuilding() {

        MatchPhraseQueryBuilder qb = getBuilder(Map.of("slop", 2, "zero_terms_query", "none"));
        assertThat(qb.slop(), equalTo(2));
        assertThat(qb.zeroTermsQuery(), equalTo(ZeroTermsQueryOption.NONE));

        Exception e = expectThrows(IllegalArgumentException.class, () -> getBuilder(Map.of("pizza", "yummy")));
        assertThat(e.getMessage(), equalTo("illegal match_phrase option [pizza]"));

        e = expectThrows(NumberFormatException.class, () -> getBuilder(Map.of("slop", "mushrooms")));
        assertThat(e.getMessage(), equalTo("For input string: \"mushrooms\""));

        e = expectThrows(ElasticsearchException.class, () -> getBuilder(Map.of("zero_terms_query", "pepperoni")));
        assertThat(e.getMessage(), equalTo("unknown serialized type [pepperoni]"));
    }

    private static MatchPhraseQueryBuilder getBuilder(Map<String, Object> options) {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final MatchPhraseQuery mpq = new MatchPhraseQuery(source, "eggplant", "foo bar", options);
        return (MatchPhraseQueryBuilder) mpq.asBuilder();
    }

    public void testToString() {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final MatchPhraseQuery mpq = new MatchPhraseQuery(source, "eggplant", "foo bar");
        assertEquals("MatchPhraseQuery@1:2[eggplant:foo bar]", mpq.toString());
    }
}
