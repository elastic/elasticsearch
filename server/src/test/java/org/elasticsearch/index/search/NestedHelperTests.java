/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search;

import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptyMap;

public class NestedHelperTests extends ESSingleNodeTestCase {

    IndexService indexService;
    MapperService mapperService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("foo2")
                            .field("type", "long")
                        .endObject()
                        .startObject("nested1")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("foo")
                                    .field("type", "keyword")
                                .endObject()
                                .startObject("foo2")
                                    .field("type", "long")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("nested2")
                            .field("type", "nested")
                            .field("include_in_parent", true)
                            .startObject("properties")
                                .startObject("foo")
                                    .field("type", "keyword")
                                .endObject()
                                .startObject("foo2")
                                    .field("type", "long")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("nested3")
                            .field("type", "nested")
                            .field("include_in_root", true)
                            .startObject("properties")
                                .startObject("foo")
                                    .field("type", "keyword")
                                .endObject()
                                .startObject("foo2")
                                    .field("type", "long")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().endObject();
        indexService = createIndex("index", Settings.EMPTY, mapping);
        mapperService = indexService.mapperService();
    }

    private static NestedHelper buildNestedHelper(MapperService mapperService) {
        return new NestedHelper(mapperService.mappingLookup().objectMappers()::get, field -> mapperService.fieldType(field) != null);
    }

    public void testMatchAll() {
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(new MatchAllDocsQuery()));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested_missing"));
    }

    public void testMatchNo() {
        assertFalse(buildNestedHelper(mapperService).mightMatchNestedDocs(new MatchNoDocsQuery()));
        assertFalse(buildNestedHelper(mapperService).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested1"));
        assertFalse(buildNestedHelper(mapperService).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested2"));
        assertFalse(buildNestedHelper(mapperService).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested3"));
        assertFalse(buildNestedHelper(mapperService).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested_missing"));
    }

    public void testTermsQuery() {
        Query termsQuery = mapperService.fieldType("foo").termsQuery(Collections.singletonList("bar"), null);
        assertFalse(buildNestedHelper(mapperService).mightMatchNestedDocs(termsQuery));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested_missing"));

        termsQuery = mapperService.fieldType("nested1.foo").termsQuery(Collections.singletonList("bar"), null);
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(termsQuery));
        assertFalse(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested_missing"));

        termsQuery = mapperService.fieldType("nested2.foo").termsQuery(Collections.singletonList("bar"), null);
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(termsQuery));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested_missing"));

        termsQuery = mapperService.fieldType("nested3.foo").termsQuery(Collections.singletonList("bar"), null);
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(termsQuery));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termsQuery, "nested_missing"));
    }

    public void testTermQuery() {
        Query termQuery = mapperService.fieldType("foo").termQuery("bar", null);
        assertFalse(buildNestedHelper(mapperService).mightMatchNestedDocs(termQuery));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested_missing"));

        termQuery = mapperService.fieldType("nested1.foo").termQuery("bar", null);
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(termQuery));
        assertFalse(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested_missing"));

        termQuery = mapperService.fieldType("nested2.foo").termQuery("bar", null);
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(termQuery));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested_missing"));

        termQuery = mapperService.fieldType("nested3.foo").termQuery("bar", null);
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(termQuery));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(termQuery, "nested_missing"));
    }

    public void testRangeQuery() {
        SearchExecutionContext context = createSearchContext(indexService).getSearchExecutionContext();
        Query rangeQuery = mapperService.fieldType("foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertFalse(buildNestedHelper(mapperService).mightMatchNestedDocs(rangeQuery));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));

        rangeQuery = mapperService.fieldType("nested1.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(rangeQuery));
        assertFalse(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));

        rangeQuery = mapperService.fieldType("nested2.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(rangeQuery));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));

        rangeQuery = mapperService.fieldType("nested3.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(rangeQuery));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));
    }

    public void testDisjunction() {
        BooleanQuery bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
                .build();
        assertFalse(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested1.foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("nested1.foo", "baz")), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertFalse(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested2.foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("nested2.foo", "baz")), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested3.foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("nested3.foo", "baz")), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested3"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
                .add(new MatchAllDocsQuery(), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested1.foo", "bar")), Occur.SHOULD)
                .add(new MatchAllDocsQuery(), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested2.foo", "bar")), Occur.SHOULD)
                .add(new MatchAllDocsQuery(), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested3.foo", "bar")), Occur.SHOULD)
                .add(new MatchAllDocsQuery(), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested3"));
    }

    private static Occur requiredOccur() {
        return random().nextBoolean() ? Occur.MUST : Occur.FILTER;
    }

    public void testConjunction() {
        BooleanQuery bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("foo", "bar")), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertFalse(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested1.foo", "bar")), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertFalse(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested2.foo", "bar")), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested3.foo", "bar")), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested3"));

        bq = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(mapperService).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(bq, "nested3"));
    }

    public void testNested() throws IOException {
        SearchExecutionContext context = indexService.newSearchExecutionContext(
            0,
            0,
            new IndexSearcher(new MultiReader()),
            () -> 0,
            null,
            emptyMap()
        );
        NestedQueryBuilder queryBuilder = new NestedQueryBuilder("nested1", new MatchAllQueryBuilder(), ScoreMode.Avg);
        ESToParentBlockJoinQuery query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        Query expectedChildQuery = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), Occur.MUST)
                // we automatically add a filter since the inner query might match non-nested docs
                .add(new TermQuery(new Term("_nested_path", "nested1")), Occur.FILTER)
                .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(buildNestedHelper(mapperService).mightMatchNestedDocs(query));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested_missing"));

        queryBuilder = new NestedQueryBuilder("nested1", new TermQueryBuilder("nested1.foo", "bar"), ScoreMode.Avg);
        query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // this time we do not add a filter since the inner query only matches inner docs
        expectedChildQuery = new TermQuery(new Term("nested1.foo", "bar"));
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(buildNestedHelper(mapperService).mightMatchNestedDocs(query));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested_missing"));

        queryBuilder = new NestedQueryBuilder("nested2", new TermQueryBuilder("nested2.foo", "bar"), ScoreMode.Avg);
        query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // we need to add the filter again because of include_in_parent
        expectedChildQuery = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested2.foo", "bar")), Occur.MUST)
                .add(new TermQuery(new Term("_nested_path", "nested2")), Occur.FILTER)
                .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(buildNestedHelper(mapperService).mightMatchNestedDocs(query));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested_missing"));

        queryBuilder = new NestedQueryBuilder("nested3", new TermQueryBuilder("nested3.foo", "bar"), ScoreMode.Avg);
        query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // we need to add the filter again because of include_in_root
        expectedChildQuery = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested3.foo", "bar")), Occur.MUST)
                .add(new TermQuery(new Term("_nested_path", "nested3")), Occur.FILTER)
                .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(buildNestedHelper(mapperService).mightMatchNestedDocs(query));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(buildNestedHelper(mapperService).mightMatchNonNestedDocs(query, "nested_missing"));
    }
}
