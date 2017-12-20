/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MatchQueryTests extends ESTestCase {

    public void testQueryBuilding() {
        MatchQueryBuilder qb = getBuilder("lenient=true");
        assertThat(qb.lenient(), equalTo(true));

        qb = getBuilder("lenient=true;operator=AND");
        assertThat(qb.lenient(), equalTo(true));
        assertThat(qb.operator(), equalTo(Operator.AND));

        Exception e = expectThrows(IllegalArgumentException.class, () -> getBuilder("pizza=yummy"));
        assertThat(e.getMessage(), equalTo("illegal match option [pizza]"));

        e = expectThrows(IllegalArgumentException.class, () -> getBuilder("operator=aoeu"));
        assertThat(e.getMessage(), equalTo("No enum constant org.elasticsearch.index.query.Operator.AOEU"));
    }

    private static MatchQueryBuilder getBuilder(String options) {
        final Location location = new Location(1, 1);
        final MatchQueryPredicate mmqp = new MatchQueryPredicate(location, null, "eggplant", options);
        final MatchQuery mmq = new MatchQuery(location, "eggplant", "foo", mmqp);
        return (MatchQueryBuilder) mmq.asBuilder();
    }
}
