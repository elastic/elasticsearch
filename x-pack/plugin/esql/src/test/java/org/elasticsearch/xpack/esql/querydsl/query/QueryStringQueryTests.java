/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class QueryStringQueryTests extends ESTestCase {

    public void testQueryBuilding() {
        QueryStringQueryBuilder qb = getBuilder(Map.of("lenient", true));
        assertThat(qb.lenient(), equalTo(true));

        qb = getBuilder(Map.of("lenient", true, "default_operator", "AND"));
        assertThat(qb.lenient(), equalTo(true));
        assertThat(qb.defaultOperator(), equalTo(Operator.AND));

        Exception e = expectThrows(IllegalArgumentException.class, () -> getBuilder(Map.of("pizza", "yummy")));
        assertThat(e.getMessage(), equalTo("illegal query_string option [pizza]"));

        e = expectThrows(ElasticsearchParseException.class, () -> getBuilder(Map.of("type", "aoeu")));
        assertThat(e.getMessage(), equalTo("failed to parse [multi_match] query type [aoeu]. unknown type."));
    }

    private static QueryStringQueryBuilder getBuilder(Map<String, Object> options) {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final QueryStringQuery query = new QueryStringQuery(source, "eggplant", Collections.singletonMap("foo", 1.0f), options);
        return (QueryStringQueryBuilder) query.toQueryBuilder();
    }

    public void testToString() {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final QueryStringQuery mmq = new QueryStringQuery(source, "eggplant", Collections.singletonMap("foo", 1.0f), Map.of());
        assertEquals("QueryStringQuery@1:2[{foo=1.0}:eggplant]", mmq.toString());
    }
}
