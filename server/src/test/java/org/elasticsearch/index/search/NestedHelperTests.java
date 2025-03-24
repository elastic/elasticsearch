/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.mock;

public class NestedHelperTests extends MapperServiceTestCase {

    MapperService mapperService;

    SearchExecutionContext searchExecutionContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        String mapping = """
            { "_doc" : {
              "properties" : {
                "foo" : { "type" : "keyword" },
                "foo2" : { "type" : "long" },
                "nested1" : {
                  "type" : "nested",
                  "properties" : {
                    "foo" : { "type" : "keyword" },
                    "foo2" : { "type" : "long" }
                  }
                },
                "nested2" : {
                  "type" : "nested",
                  "include_in_parent" : true,
                  "properties": {
                    "foo" : { "type" : "keyword" },
                    "foo2" : { "type" : "long" }
                  }
                },
                "nested3" : {
                  "type" : "nested",
                  "include_in_root" : true,
                  "properties": {
                    "foo" : { "type" : "keyword" },
                    "foo2" : { "type" : "long" }
                  }
                }
              }
            } }
            """;
        mapperService = createMapperService(mapping);
        searchExecutionContext = new SearchExecutionContext(
            0,
            0,
            mapperService.getIndexSettings(),
            null,
            null,
            mapperService,
            mapperService.mappingLookup(),
            null,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            null,
            System::currentTimeMillis,
            null,
            null,
            () -> true,
            null,
            emptyMap(),
            MapperMetrics.NOOP
        );
    }

    public void testMatchAll() {
        assertTrue(NestedHelper.mightMatchNestedDocs(new MatchAllDocsQuery(), searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested_missing", searchExecutionContext));
    }

    public void testMatchNo() {
        assertFalse(NestedHelper.mightMatchNestedDocs(new MatchNoDocsQuery(), searchExecutionContext));
        assertFalse(NestedHelper.mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested1", searchExecutionContext));
        assertFalse(NestedHelper.mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested2", searchExecutionContext));
        assertFalse(NestedHelper.mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested3", searchExecutionContext));
        assertFalse(NestedHelper.mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested_missing", searchExecutionContext));
    }

    public void testTermsQuery() {
        Query termsQuery = mapperService.fieldType("foo").termsQuery(Collections.singletonList("bar"), null);
        assertFalse(NestedHelper.mightMatchNestedDocs(termsQuery, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested_missing", searchExecutionContext));

        termsQuery = mapperService.fieldType("nested1.foo").termsQuery(Collections.singletonList("bar"), null);
        assertTrue(NestedHelper.mightMatchNestedDocs(termsQuery, searchExecutionContext));
        assertFalse(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested_missing", searchExecutionContext));

        termsQuery = mapperService.fieldType("nested2.foo").termsQuery(Collections.singletonList("bar"), null);
        assertTrue(NestedHelper.mightMatchNestedDocs(termsQuery, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested_missing", searchExecutionContext));

        termsQuery = mapperService.fieldType("nested3.foo").termsQuery(Collections.singletonList("bar"), null);
        assertTrue(NestedHelper.mightMatchNestedDocs(termsQuery, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termsQuery, "nested_missing", searchExecutionContext));
    }

    public void testTermQuery() {
        Query termQuery = mapperService.fieldType("foo").termQuery("bar", null);
        assertFalse(NestedHelper.mightMatchNestedDocs(termQuery, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested_missing", searchExecutionContext));

        termQuery = mapperService.fieldType("nested1.foo").termQuery("bar", null);
        assertTrue(NestedHelper.mightMatchNestedDocs(termQuery, searchExecutionContext));
        assertFalse(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested_missing", searchExecutionContext));

        termQuery = mapperService.fieldType("nested2.foo").termQuery("bar", null);
        assertTrue(NestedHelper.mightMatchNestedDocs(termQuery, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested_missing", searchExecutionContext));

        termQuery = mapperService.fieldType("nested3.foo").termQuery("bar", null);
        assertTrue(NestedHelper.mightMatchNestedDocs(termQuery, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(termQuery, "nested_missing", searchExecutionContext));
    }

    public void testRangeQuery() {
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        Query rangeQuery = mapperService.fieldType("foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertFalse(NestedHelper.mightMatchNestedDocs(rangeQuery, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested_missing", searchExecutionContext));

        rangeQuery = mapperService.fieldType("nested1.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(NestedHelper.mightMatchNestedDocs(rangeQuery, searchExecutionContext));
        assertFalse(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested_missing", searchExecutionContext));

        rangeQuery = mapperService.fieldType("nested2.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(NestedHelper.mightMatchNestedDocs(rangeQuery, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested_missing", searchExecutionContext));

        rangeQuery = mapperService.fieldType("nested3.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(NestedHelper.mightMatchNestedDocs(rangeQuery, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(rangeQuery, "nested_missing", searchExecutionContext));
    }

    public void testDisjunction() {
        BooleanQuery bq = new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
            .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
            .build();
        assertFalse(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested1", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested1.foo", "bar")), Occur.SHOULD)
            .add(new TermQuery(new Term("nested1.foo", "baz")), Occur.SHOULD)
            .build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertFalse(NestedHelper.mightMatchNonNestedDocs(bq, "nested1", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested2.foo", "bar")), Occur.SHOULD)
            .add(new TermQuery(new Term("nested2.foo", "baz")), Occur.SHOULD)
            .build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested2", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested3.foo", "bar")), Occur.SHOULD)
            .add(new TermQuery(new Term("nested3.foo", "baz")), Occur.SHOULD)
            .build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested3", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
            .add(new MatchAllDocsQuery(), Occur.SHOULD)
            .build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested1", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested1.foo", "bar")), Occur.SHOULD)
            .add(new MatchAllDocsQuery(), Occur.SHOULD)
            .build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested1", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested2.foo", "bar")), Occur.SHOULD)
            .add(new MatchAllDocsQuery(), Occur.SHOULD)
            .build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested2", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested3.foo", "bar")), Occur.SHOULD)
            .add(new MatchAllDocsQuery(), Occur.SHOULD)
            .build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested3", searchExecutionContext));
    }

    private static Occur requiredOccur() {
        return random().nextBoolean() ? Occur.MUST : Occur.FILTER;
    }

    public void testConjunction() {
        BooleanQuery bq = new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), requiredOccur())
            .add(new MatchAllDocsQuery(), requiredOccur())
            .build();
        assertFalse(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested1", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested1.foo", "bar")), requiredOccur())
            .add(new MatchAllDocsQuery(), requiredOccur())
            .build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertFalse(NestedHelper.mightMatchNonNestedDocs(bq, "nested1", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested2.foo", "bar")), requiredOccur())
            .add(new MatchAllDocsQuery(), requiredOccur())
            .build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested2", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new TermQuery(new Term("nested3.foo", "bar")), requiredOccur())
            .add(new MatchAllDocsQuery(), requiredOccur())
            .build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested3", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), requiredOccur()).add(new MatchAllDocsQuery(), requiredOccur()).build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested1", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), requiredOccur()).add(new MatchAllDocsQuery(), requiredOccur()).build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested1", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), requiredOccur()).add(new MatchAllDocsQuery(), requiredOccur()).build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested2", searchExecutionContext));

        bq = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), requiredOccur()).add(new MatchAllDocsQuery(), requiredOccur()).build();
        assertTrue(NestedHelper.mightMatchNestedDocs(bq, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(bq, "nested3", searchExecutionContext));
    }

    public void testNested() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        NestedQueryBuilder queryBuilder = new NestedQueryBuilder("nested1", new MatchAllQueryBuilder(), ScoreMode.Avg);
        ESToParentBlockJoinQuery query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        Query expectedChildQuery = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.MUST)
            // we automatically add a filter since the inner query might match non-nested docs
            .add(new TermQuery(new Term("_nested_path", "nested1")), Occur.FILTER)
            .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(NestedHelper.mightMatchNestedDocs(query, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested_missing", searchExecutionContext));

        queryBuilder = new NestedQueryBuilder("nested1", new TermQueryBuilder("nested1.foo", "bar"), ScoreMode.Avg);
        query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // this time we do not add a filter since the inner query only matches inner docs
        expectedChildQuery = new TermQuery(new Term("nested1.foo", "bar"));
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(NestedHelper.mightMatchNestedDocs(query, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested_missing", searchExecutionContext));

        queryBuilder = new NestedQueryBuilder("nested2", new TermQueryBuilder("nested2.foo", "bar"), ScoreMode.Avg);
        query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // we need to add the filter again because of include_in_parent
        expectedChildQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("nested2.foo", "bar")), Occur.MUST)
            .add(new TermQuery(new Term("_nested_path", "nested2")), Occur.FILTER)
            .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(NestedHelper.mightMatchNestedDocs(query, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested_missing", searchExecutionContext));

        queryBuilder = new NestedQueryBuilder("nested3", new TermQueryBuilder("nested3.foo", "bar"), ScoreMode.Avg);
        query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // we need to add the filter again because of include_in_root
        expectedChildQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("nested3.foo", "bar")), Occur.MUST)
            .add(new TermQuery(new Term("_nested_path", "nested3")), Occur.FILTER)
            .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(NestedHelper.mightMatchNestedDocs(query, searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested1", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested2", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested3", searchExecutionContext));
        assertTrue(NestedHelper.mightMatchNonNestedDocs(query, "nested_missing", searchExecutionContext));
    }
}
