/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.equalTo;

public class MatchQueryTests extends ESTestCase {
    static MatchQuery randomMatchQuery() {
        return new MatchQuery(SourceTests.randomSource(), randomAlphaOfLength(5), randomAlphaOfLength(5));
        // TODO add the predicate
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomMatchQuery(), MatchQueryTests::copy, MatchQueryTests::mutate);
    }

    private static MatchQuery copy(MatchQuery query) {
        return new MatchQuery(query.source(), query.name(), query.text(), query.options());
    }

    private static MatchQuery mutate(MatchQuery query) {
        List<Function<MatchQuery, MatchQuery>> options = Arrays.asList(
            q -> new MatchQuery(SourceTests.mutate(q.source()), q.name(), q.text(), q.options()),
            q -> new MatchQuery(q.source(), randomValueOtherThan(q.name(), () -> randomAlphaOfLength(5)), q.text(), q.options()),
            q -> new MatchQuery(q.source(), q.name(), randomValueOtherThan(q.text(), () -> randomAlphaOfLength(5)), q.options())
        );
        // TODO mutate the predicate
        return randomFrom(options).apply(query);
    }

    public void testQueryBuilding() {

        MatchQueryBuilder qb = getBuilder(Map.of("lenient", true, "operator", "AND"));
        assertThat(qb.lenient(), equalTo(true));
        assertThat(qb.operator(), equalTo(Operator.AND));

        Exception e = expectThrows(IllegalArgumentException.class, () -> getBuilder(Map.of("pizza", "yummy")));
        assertThat(e.getMessage(), equalTo("illegal match option [pizza]"));

        e = expectThrows(IllegalArgumentException.class, () -> getBuilder(Map.of("operator", "aoeu")));
        assertThat(e.getMessage(), equalTo("No enum constant org.elasticsearch.index.query.Operator.AOEU"));
    }

    private static MatchQueryBuilder getBuilder(Map<String, Object> options) {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", KEYWORD, emptyMap(), true));
        final MatchQuery mmq = new MatchQuery(source, "eggplant", "foo", options);
        return (MatchQueryBuilder) mmq.asBuilder();
    }

    public void testToString() {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", KEYWORD, emptyMap(), true));
        final MatchQuery mmq = new MatchQuery(source, "eggplant", "foo");
        assertEquals("MatchQuery@1:2[eggplant:foo]", mmq.toString());
    }
}
