/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.apache.lucene.sandbox.search.CombinedFieldQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.index.query.CombinedFieldsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.relevancesearch.RelevanceSearchPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelevanceMatchQueryBuilderTests extends AbstractQueryTestCase<RelevanceMatchQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> classpathPlugins = new ArrayList<>();
        classpathPlugins.add(RelevanceSearchPlugin.class);
        return classpathPlugins;
    }

    public void testQueryParsing() throws IOException {
        String json = """
            {
              "relevance_match" : {
                "query" : "quick brown fox"
              }
            }""";
        RelevanceMatchQueryBuilder parsed = (RelevanceMatchQueryBuilder) parseQuery(json);

        assertEquals(json, "quick brown fox", parsed.getQuery());
    }

    public void testRelevanceSettingsNotEmpty() throws IOException {
        String json = """
            {
              "relevance_match" : {
                "query" : "quick brown fox",
                "relevance_settings": ""
              }
            }""";
        ParsingException ex = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[relevance_match] relevance_settings must have at least one character in length", ex.getMessage());
    }

    public void testCurationSettingsNotEmpty() throws IOException {
        String json = """
            {
              "relevance_match" : {
                "query" : "quick brown fox",
                "curations": ""
              }
            }""";
        ParsingException ex = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[relevance_match] curations must have at least one character in length", ex.getMessage());
    }

    public void testOptionalParsing() throws IOException {
        String json = """
            {
              "relevance_match" : {
                "query" : "quick brown fox",
                "relevance_settings": "test-settings",
                "curations": "test-curations"
              }
            }""";
        RelevanceMatchQueryBuilder parsed = (RelevanceMatchQueryBuilder) parseQuery(json);

        assertEquals(json, "quick brown fox", parsed.getQuery());
        assertEquals(json, "test-settings", parsed.getRelevanceSettingsId());
        assertEquals(json, "test-curations", parsed.getCurationsSettingsId());
    }

    @Override
    protected RelevanceMatchQueryBuilder doCreateTestQueryBuilder() {

        QueryBuilder resultQuery = new CombinedFieldsQueryBuilder("test", "field 1", "field2");
        RelevanceMatchQueryRewriter queryRewriter = mock(RelevanceMatchQueryRewriter.class);

        try {
            when(queryRewriter.rewriteQuery(any(), any())).thenReturn(resultQuery);
        } catch (IOException e) {
            // Can't happen on mock definition
        }

        RelevanceMatchQueryBuilder queryBuilder = new RelevanceMatchQueryBuilder(queryRewriter);

        queryBuilder.setQuery(getRandomQueryText());

        if (randomBoolean()) {
            final String relevanceSettingsId = randomAlphaOfLengthBetween(1, 10);
            queryBuilder.setRelevanceSettingsId(relevanceSettingsId);
        }

        if (randomBoolean()) {
            final String curationsSettingsId = randomAlphaOfLengthBetween(1, 10);
            queryBuilder.setCurationsSettingsId(curationsSettingsId);
        }

        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(RelevanceMatchQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        assertThat(
            query,
            anyOf(
                Arrays.asList(
                    // Curations
                    instanceOf(BooleanQuery.class),
                    // Query is empty, we have no current ZeroTermsQueryOption selection,
                    // and default is issuing a no match query for no terms
                    instanceOf(MatchNoDocsQuery.class),
                    // No curations
                    instanceOf(CombinedFieldQuery.class)
                )
            )
        );
    }

    @Override
    protected QueryBuilder rewriteQuery(RelevanceMatchQueryBuilder queryBuilder, QueryRewriteContext rewriteContext) throws IOException {
        // TODO Need to check whether this is a valid approach. How would query rewriting work?
        // Is it possible to inject the needed services in other ways?
        RelevanceMatchQueryBuilder rewritten = (RelevanceMatchQueryBuilder) super.rewriteQuery(queryBuilder, rewriteContext);

        rewritten.setQueryRewriter(queryBuilder.getQueryRewriter());

        return rewritten;
    }

    @Override
    public void testToQuery() throws IOException {
        /* TODO Can't run testToQuery as it does copy queries using serialization / deserialization.
        In the deserialization phase, the RelevanceSearchPlugin.getQueries() method is invoked, and in turn the constructor for
         RelevanceMatchQueryBuilder is invoked.
        However, services have not been created as plugin creation is mocked, and thus createComponents() is not invoked.
        We probably have a way around this, but it seems that QueryBuilders should not rely on external services, or we need to
        find a way of having these services created.

        Thought: Override copyQuery() method (private -> protected), and do something similar to rewriteQuery.
        This is something that plugins should provide, but do not because of the plugins mocking
         */
    }
}
