/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BestEffortQueryBuilderTests extends MapperServiceTestCase {

    private static final QueryBuilder TERM = QueryBuilders.termQuery("k", "a");
    private static final BestEffortQueryBuilder QUERY = new BestEffortQueryBuilder(
        TERM,
        List.of(new BestEffortQueryBuilder.Field("k", DataType.KEYWORD, false))
    );

    /** A shard that can query the field runs the query. */
    public void testShardThatCanQueryRunsTheQuery() throws IOException {
        SearchExecutionContext context = context("""
            { "_doc": { "properties": { "k": { "type": "keyword" } } } }""");
        assertThat(QUERY.toQuery(context), equalTo(TERM.toQuery(context)));
    }

    /** A shard that cannot query the field matches every document, and scores nothing. */
    public void testShardThatCannotQueryMatchesEveryDocument() throws IOException {
        SearchExecutionContext context = context("""
            { "_doc": { "properties": { "k": { "type": "keyword", "doc_values": false } } } }""");
        Query query = QUERY.toQuery(context);
        assertThat(query, instanceOf(BoostQuery.class));
        assertThat(((BoostQuery) query).getQuery(), instanceOf(MatchAllDocsQuery.class));
        assertThat(((BoostQuery) query).getBoost(), equalTo(0f));
    }

    private SearchExecutionContext context(String mapping) throws IOException {
        return createSearchExecutionContext(createMapperService(mapping));
    }
}
