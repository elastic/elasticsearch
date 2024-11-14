/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.kql.query.KqlQueryBuilder;

import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;

public class KqlQueryTests extends ESTestCase {
    static KqlQuery randomMatchQuery() {
        return new KqlQuery(SourceTests.randomSource(), randomAlphaOfLength(5));
        // TODO add the options
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomMatchQuery(), KqlQueryTests::copy, KqlQueryTests::mutate);
    }

    private static KqlQuery copy(KqlQuery query) {
        return new KqlQuery(query.source(), query.query(), query.options());
    }

    private static KqlQuery mutate(KqlQuery query) {
        List<Function<KqlQuery, KqlQuery>> options = Arrays.asList(
            q -> new KqlQuery(SourceTests.mutate(q.source()), q.query(), q.options()),
            q -> new KqlQuery(q.source(), randomValueOtherThan(q.query(), () -> randomAlphaOfLength(5)), q.options())
        );
        // TODO mutate the options
        return randomFrom(options).apply(query);
    }

    public void testQueryBuilding() {
        KqlQueryBuilder qb = getBuilder("case_insensitive=false");
        assertThat(qb.caseInsensitive(), equalTo(false));

        qb = getBuilder("case_insensitive=false;time_zone=UTC;default_field=foo");
        assertThat(qb.caseInsensitive(), equalTo(false));
        assertThat(qb.timeZone(), equalTo(ZoneId.of("UTC")));
        assertThat(qb.defaultField(), equalTo("foo"));

        Exception e = expectThrows(IllegalArgumentException.class, () -> getBuilder("pizza=yummy"));
        assertThat(e.getMessage(), equalTo("illegal kql query option [pizza]"));

        e = expectThrows(ZoneRulesException.class, () -> getBuilder("time_zone=aoeu"));
        assertThat(e.getMessage(), equalTo("Unknown time-zone ID: aoeu"));
    }

    private static KqlQueryBuilder getBuilder(String options) {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final StringQueryPredicate predicate = new StringQueryPredicate(source, "eggplant", options);
        final KqlQuery kqlQuery = new KqlQuery(source, "eggplant", predicate.optionMap());
        return (KqlQueryBuilder) kqlQuery.asBuilder();
    }

    public void testToString() {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final KqlQuery kqlQuery = new KqlQuery(source, "eggplant", Map.of());
        assertEquals("KqlQuery@1:2[eggplant]", kqlQuery.toString());
    }
}
