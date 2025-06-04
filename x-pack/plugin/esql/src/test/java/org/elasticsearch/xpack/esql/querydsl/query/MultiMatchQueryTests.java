/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MultiMatchQueryTests extends ESTestCase {

    public void testQueryBuilding() {
        MultiMatchQueryBuilder qb = getBuilder(Map.of("lenient", true));
        assertThat(qb.lenient(), equalTo(true));

        qb = getBuilder(Map.of("type", "best_fields"));
        assertThat(qb.getType(), equalTo(MultiMatchQueryBuilder.Type.BEST_FIELDS));

        Exception e = expectThrows(IllegalArgumentException.class, () -> getBuilder(Map.of("pizza", "yummy")));
        assertThat(e.getMessage(), equalTo("illegal multi_match option [pizza]"));

        e = expectThrows(ElasticsearchParseException.class, () -> getBuilder(Map.of("type", "aoeu")));
        assertThat(e.getMessage(), equalTo("failed to parse [multi_match] query type [aoeu]. unknown type."));
    }

    private static MultiMatchQueryBuilder getBuilder(Map<String, Object> options) {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final MultiMatchQuery mmq = new MultiMatchQuery(source, "eggplant", Map.of("bar", 1.0f, "foo", 1.0f), options);
        return (MultiMatchQueryBuilder) mmq.asBuilder();
    }

    public void testToString() {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final MultiMatchQuery mmq = new MultiMatchQuery(source, "eggplant", Map.of("bar", 1.0f, "foo", 1.0f), null);
        assertEquals("MultiMatchQuery@1:2[{bar=1.0, foo=1.0}:eggplant]", mmq.toString());
    }
}
