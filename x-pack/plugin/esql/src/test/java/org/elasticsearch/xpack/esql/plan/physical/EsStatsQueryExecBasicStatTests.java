/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.BasicStat;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.StatsType;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class EsStatsQueryExecBasicStatTests extends ESTestCase {

    /**
     * When the source query already requires the count's query, the combination is the source query itself. The count must
     * not set its boost, since other fragments can use the same source query.
     */
    public void testFilterDoesNotBoostTheSourceQuery() {
        QueryBuilder exists = QueryBuilders.existsQuery("f");
        BoolQueryBuilder source = QueryBuilders.boolQuery().filter(exists).filter(QueryBuilders.termQuery("g", "v"));
        BasicStat stat = new BasicStat("c", StatsType.COUNT, QueryBuilders.existsQuery("f"));

        QueryBuilder filtered = stat.filter(source);

        assertThat(source.boost(), equalTo(AbstractQueryBuilder.DEFAULT_BOOST));
        assertThat(filtered, not(sameInstance(source)));
        assertThat(filtered.boost(), equalTo(0.0f));
        assertThat(filtered, instanceOf(BoolQueryBuilder.class));
        assertThat(((BoolQueryBuilder) filtered).filter(), equalTo(List.of(source)));
    }

    public void testFilterCombinesWithTheSourceQuery() {
        QueryBuilder source = QueryBuilders.termQuery("g", "v");
        QueryBuilder exists = QueryBuilders.existsQuery("f");
        QueryBuilder filtered = new BasicStat("c", StatsType.COUNT, exists).filter(source);

        assertThat(source.boost(), equalTo(AbstractQueryBuilder.DEFAULT_BOOST));
        assertThat(filtered.boost(), equalTo(0.0f));
        assertThat(((BoolQueryBuilder) filtered).filter(), equalTo(List.of(source, exists)));
    }
}
