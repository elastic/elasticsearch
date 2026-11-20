/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class NestedQueryTests extends ESTestCase {
    public void testBuildsANestedQueryThatIgnoresUnmappedPaths() {
        NestedQuery query = new NestedQuery(Source.EMPTY, "items", new TermQuery(Source.EMPTY, "items.sku", "a"), ScoreMode.None);
        assertThat(query.toQueryBuilder(), instanceOf(NestedQueryBuilder.class));
        NestedQueryBuilder builder = (NestedQueryBuilder) query.toQueryBuilder();
        assertThat(builder.path(), equalTo("items"));
        assertThat(builder.scoreMode(), equalTo(ScoreMode.None));
        assertThat(builder.ignoreUnmapped(), is(true));
        assertThat(builder.query(), equalTo(QueryBuilders.termQuery("items.sku", "a").boost(Query.NO_SCORE_BOOST)));
    }

    public void testScoresOnlyWhenAskedAndTheChildDoes() {
        Query scorable = new Query(Source.EMPTY) {
            @Override
            protected org.elasticsearch.index.query.QueryBuilder asBuilder() {
                return QueryBuilders.matchAllQuery();
            }

            @Override
            protected String innerToString() {
                return "scorable";
            }

            @Override
            public boolean containsPlan() {
                return false;
            }

            @Override
            public boolean scorable() {
                return true;
            }
        };
        assertThat(new NestedQuery(Source.EMPTY, "items", scorable, ScoreMode.Max).scorable(), is(true));
        assertThat(new NestedQuery(Source.EMPTY, "items", scorable, ScoreMode.None).scorable(), is(false));
    }

    public void testEqualityCoversPathChildAndScoreMode() {
        Query child = new TermQuery(Source.EMPTY, "items.sku", "a");
        NestedQuery query = new NestedQuery(Source.EMPTY, "items", child, ScoreMode.None);
        assertThat(query, equalTo(new NestedQuery(Source.EMPTY, "items", child, ScoreMode.None)));
        assertThat(query.equals(new NestedQuery(Source.EMPTY, "other", child, ScoreMode.None)), is(false));
        assertThat(query.equals(new NestedQuery(Source.EMPTY, "items", child, ScoreMode.Max)), is(false));
        assertThat(
            query.equals(new NestedQuery(Source.EMPTY, "items", new TermQuery(Source.EMPTY, "items.sku", "b"), ScoreMode.None)),
            is(false)
        );
    }
}
