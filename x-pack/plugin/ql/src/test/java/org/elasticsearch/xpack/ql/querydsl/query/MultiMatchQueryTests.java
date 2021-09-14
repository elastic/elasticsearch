/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.ql.querydsl.query.MultiMatchQuery;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;

public class MultiMatchQueryTests extends ESTestCase {

    public void testQueryBuilding() {
        MultiMatchQueryBuilder qb = getBuilder("lenient=true");
        assertThat(qb.lenient(), equalTo(true));

        qb = getBuilder("type=best_fields");
        assertThat(qb.getType(), equalTo(MultiMatchQueryBuilder.Type.BEST_FIELDS));

        Exception e = expectThrows(IllegalArgumentException.class, () -> getBuilder("pizza=yummy"));
        assertThat(e.getMessage(), equalTo("illegal multi_match option [pizza]"));

        e = expectThrows(ElasticsearchParseException.class, () -> getBuilder("type=aoeu"));
        assertThat(e.getMessage(), equalTo("failed to parse [multi_match] query type [aoeu]. unknown type."));
    }

    private static MultiMatchQueryBuilder getBuilder(String options) {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final MultiMatchQueryPredicate mmqp = new MultiMatchQueryPredicate(source, "foo,bar", "eggplant", options);
        final Map<String, Float> fields = new HashMap<>();
        fields.put("foo", 1.0f);
        fields.put("bar", 1.0f);
        final MultiMatchQuery mmq = new MultiMatchQuery(source, "eggplant", fields, mmqp);
        return (MultiMatchQueryBuilder) mmq.asBuilder();
    }

    public void testToString() {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final MultiMatchQueryPredicate mmqp = new MultiMatchQueryPredicate(source, "foo,bar", "eggplant", "");
        // Use a TreeMap so we get the fields in a predictable order.
        final Map<String, Float> fields = new TreeMap<>();
        fields.put("foo", 1.0f);
        fields.put("bar", 1.0f);
        final MultiMatchQuery mmq = new MultiMatchQuery(source, "eggplant", fields, mmqp);
        assertEquals("MultiMatchQuery@1:2[{bar=1.0, foo=1.0}:eggplant]", mmq.toString());
    }
}
