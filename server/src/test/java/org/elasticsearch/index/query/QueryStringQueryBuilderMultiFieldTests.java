/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class QueryStringQueryBuilderMultiFieldTests extends MapperServiceTestCase {

    public void testToQueryFieldsWildcard() throws Exception {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "f_text" : { "type" : "text" },
                "f_keyword" : { "type" : "keyword" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "f_text" : "foo", "f_keyword" : "bar" }
            """));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {
            Query query = queryStringQuery("test").field("f*").toQuery(createSearchExecutionContext(mapperService, new IndexSearcher(ir)));
            Query expected = new DisjunctionMaxQuery(
                List.of(new TermQuery(new Term("f_text", "test")), new TermQuery(new Term("f_keyword", "test"))),
                0
            );
            assertEquals(expected, query);
        });
    }

    public void testDefaultField() throws Exception {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "f_text1" : { "type" : "text" },
                "f_text2" : { "type" : "text" },
                "f_keyword1" : { "type" : "keyword" },
                "f_keyword2" : { "type" : "keyword" },
                "f_date" : { "type" : "date" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "f_text1" : "foo", "f_text2" : "bar", "f_keyword1" : "baz", "f_keyword2" : "buz", "f_date" : "2021-12-20T00:00:00" }
            """));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {

            IndexSearcher searcher = new IndexSearcher(ir);

            {
                // default value 'index.query.default_field = *' sets leniency to true
                SearchExecutionContext context = createSearchExecutionContext(mapperService, searcher);
                Query query = new QueryStringQueryBuilder("hello").toQuery(context);
                Query expected = new DisjunctionMaxQuery(
                    List.of(
                        new TermQuery(new Term("f_text1", "hello")),
                        new TermQuery(new Term("f_text2", "hello")),
                        new TermQuery(new Term("f_keyword1", "hello")),
                        new TermQuery(new Term("f_keyword2", "hello")),
                        new MatchNoDocsQuery()
                    ),
                    0f
                );
                assertThat(query, equalTo(expected));
            }

            {
                // default field list contains '*' sets leniency to true
                Settings settings = Settings.builder().putList("index.query.default_field", "f_text1", "*").build();
                SearchExecutionContext context = createSearchExecutionContext(mapperService, searcher, settings);
                Query query = new QueryStringQueryBuilder("hello").toQuery(context);
                Query expected = new DisjunctionMaxQuery(
                    List.of(
                        new TermQuery(new Term("f_text1", "hello")),
                        new TermQuery(new Term("f_text2", "hello")),
                        new TermQuery(new Term("f_keyword1", "hello")),
                        new TermQuery(new Term("f_keyword2", "hello")),
                        new MatchNoDocsQuery()
                    ),
                    0f
                );
                assertThat(query, equalTo(expected));
            }

            {
                // default field list contains no wildcards, leniency = false
                Settings settings = Settings.builder().putList("index.query.default_field", "f_text1", "f_date").build();
                SearchExecutionContext context = createSearchExecutionContext(mapperService, searcher, settings);
                Exception e = expectThrows(Exception.class, () -> new QueryStringQueryBuilder("hello").toQuery(context));
                assertThat(e.getMessage(), containsString("failed to parse date field [hello]"));
            }

            {
                // default field list contains boost
                Settings settings = Settings.builder().putList("index.query.default_field", "f_text1", "f_text2^4").build();
                SearchExecutionContext context = createSearchExecutionContext(mapperService, searcher, settings);
                Query query = new QueryStringQueryBuilder("hello").toQuery(context);
                Query expected = new DisjunctionMaxQuery(
                    List.of(new TermQuery(new Term("f_text1", "hello")), new BoostQuery(new TermQuery(new Term("f_text2", "hello")), 4f)),
                    0f
                );
                assertThat(query, equalTo(expected));
            }

        });
    }

    public void testFieldListIncludesWildcard() throws Exception {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "f_text1" : { "type" : "text" },
                "f_text2" : { "type" : "text" },
                "f_keyword1" : { "type" : "keyword" },
                "f_keyword2" : { "type" : "keyword" },
                "f_date" : { "type" : "date" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "f_text1" : "foo", "f_text2" : "bar", "f_keyword1" : "baz", "f_keyword2" : "buz", "f_date" : "2021-12-20T00:00:00" }
            """));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {
            SearchExecutionContext context = createSearchExecutionContext(mapperService, new IndexSearcher(ir));
            Query expected = new DisjunctionMaxQuery(
                List.of(
                    new TermQuery(new Term("f_text1", "hello")),
                    new TermQuery(new Term("f_text2", "hello")),
                    new TermQuery(new Term("f_keyword1", "hello")),
                    new TermQuery(new Term("f_keyword2", "hello")),
                    new MatchNoDocsQuery()
                ),
                0f
            );
            assertEquals(expected, new QueryStringQueryBuilder("hello").field("*").toQuery(context));
            assertEquals(expected, new QueryStringQueryBuilder("hello").field("f_text1").field("*").toQuery(context));
        });
    }

    public void testMergeBoosts() throws IOException {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "f_text" : { "type" : "text" },
                "f_keyword" : { "type" : "keyword" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "f_text" : "foo", "f_keyword" : "bar" }
            """));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {
            Query query = queryStringQuery("first").type(MultiMatchQueryBuilder.Type.MOST_FIELDS)
                .field("f_text", 0.3f)
                .field("f*", 0.5f)
                .toQuery(createSearchExecutionContext(mapperService, new IndexSearcher(ir)));
            Query expected = new DisjunctionMaxQuery(
                List.of(
                    new BoostQuery(new TermQuery(new Term("f_text", "first")), 0.15f),
                    new BoostQuery(new TermQuery(new Term("f_keyword", "first")), 0.5f)
                ),
                1
            );
            assertEquals(expected, query);
        });
    }

    /**
     * Query terms that contain "now" can trigger a query to not be cacheable.
     * This test checks the search context cacheable flag is updated accordingly.
     */
    public void testCachingStrategiesWithNow() throws IOException {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "f_text1" : { "type" : "text" },
                "f_date" : { "type" : "date" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "f_text1" : "foo", "f_date" : "2021-12-20T00:00:00" }
            """));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {
            IndexSearcher searcher = new IndexSearcher(ir);

            // if we hit all fields, this should contain a date field and should disable cachability
            String query = "now " + randomAlphaOfLengthBetween(4, 10);
            QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder(query);
            assertQueryCachability(queryBuilder, createSearchExecutionContext(mapperService, searcher), false);

            // querying the date field directly with 'now' disables cachability
            queryBuilder = new QueryStringQueryBuilder("now").field("f_date");
            assertQueryCachability(queryBuilder, createSearchExecutionContext(mapperService, searcher), false);

            // but it's only lower case that matters
            query = randomFrom("NoW", "nOw", "NOW") + " " + randomAlphaOfLengthBetween(4, 10);
            queryBuilder = new QueryStringQueryBuilder(query);
            assertQueryCachability(queryBuilder, createSearchExecutionContext(mapperService, searcher), true);
        });
    }

    private void assertQueryCachability(QueryStringQueryBuilder qb, SearchExecutionContext context, boolean cachingExpected)
        throws IOException {
        assert context.isCacheable();
        /*
         * We use a private rewrite context here since we want the most realistic way of asserting that we are cacheable or not. We do it
         * this way in SearchService where we first rewrite the query with a private context, then reset the context and then build the
         * actual lucene query
         */
        PlainActionFuture<QueryBuilder> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(qb, new SearchExecutionContext(context), future);
        QueryBuilder rewritten = future.actionGet();
        assertNotNull(rewritten.toQuery(context));
        assertEquals(
            "query should " + (cachingExpected ? "" : "not") + " be cacheable: " + qb.toString(),
            cachingExpected,
            context.isCacheable()
        );
    }
}
