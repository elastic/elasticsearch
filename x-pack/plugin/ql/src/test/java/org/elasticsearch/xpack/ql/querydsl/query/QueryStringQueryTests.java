/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.ql.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class QueryStringQueryTests extends ESTestCase {

    public void testQueryBuilding() {
        QueryStringQueryBuilder qb = getBuilder("lenient=true");
        assertThat(qb.lenient(), equalTo(true));

        qb = getBuilder("lenient=true;default_operator=AND");
        assertThat(qb.lenient(), equalTo(true));
        assertThat(qb.defaultOperator(), equalTo(Operator.AND));

        Exception e = expectThrows(IllegalArgumentException.class, () -> getBuilder("pizza=yummy"));
        assertThat(e.getMessage(), equalTo("illegal query_string option [pizza]"));

        e = expectThrows(ElasticsearchParseException.class, () -> getBuilder("type=aoeu"));
        assertThat(e.getMessage(), equalTo("failed to parse [multi_match] query type [aoeu]. unknown type."));
    }

    private static QueryStringQueryBuilder getBuilder(String options) {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final StringQueryPredicate mmqp = new StringQueryPredicate(source, "eggplant", options);
        final QueryStringQuery mmq = new QueryStringQuery(source, "eggplant", Collections.singletonMap("foo", 1.0f), mmqp);
        return (QueryStringQueryBuilder) mmq.asBuilder();
    }


    public void testToString() {
        final Source source = new Source(1, 1, StringUtils.EMPTY);
        final StringQueryPredicate mmqp = new StringQueryPredicate(source, "eggplant", "");
        final QueryStringQuery mmq = new QueryStringQuery(source, "eggplant", Collections.singletonMap("foo", 1.0f), mmqp);
        assertEquals("QueryStringQuery@1:2[{foo=1.0}:eggplant]", mmq.toString());
    }
}
