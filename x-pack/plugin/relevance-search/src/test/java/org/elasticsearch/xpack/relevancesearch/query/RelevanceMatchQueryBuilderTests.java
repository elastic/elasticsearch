/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.lucene.sandbox.search.CombinedFieldQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.relevancesearch.RelevanceSearchPlugin;
import org.elasticsearch.xpack.relevancesearch.relevance.curations.Condition;
import org.elasticsearch.xpack.relevancesearch.relevance.curations.CurationSettings;
import org.elasticsearch.xpack.relevancesearch.relevance.curations.CurationsService;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettings;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettingsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelevanceMatchQueryBuilderTests extends AbstractQueryTestCase<RelevanceMatchQueryBuilder> {


    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

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
        final RelevanceMatchQueryBuilder builder = new RelevanceMatchQueryBuilder();

        RelevanceSettingsService relevanceSettingsService = mock(RelevanceSettingsService.class);
        CurationsService curationsService = mock(CurationsService.class);

        builder.setQuery(getRandomQueryText());
        if (randomBoolean()) {
            final String relevanceSettingsId = randomAlphaOfLengthBetween(1, 10);
            builder.setRelevanceSettingsId(relevanceSettingsId);

            RelevanceSettings relevanceSettings = new RelevanceSettings();
            relevanceSettings.setFields(List.of(generateRandomStringArray(10, 10, false, false)));
            try {
                when(relevanceSettingsService.getRelevanceSettings(eq(relevanceSettingsId))).thenReturn(relevanceSettings);
                when(relevanceSettingsService.getRelevanceSettings(not(eq(relevanceSettingsId)))).thenThrow(
                    new RelevanceSettingsService.RelevanceSettingsNotFoundException("Relevance settings not found")
                );
            } catch (RelevanceSettingsService.RelevanceSettingsNotFoundException e) {
                // Can't happen defining mock
            }

        }
        if (randomBoolean()) {
            final String curationsSettingsId = randomAlphaOfLengthBetween(1, 10);
            builder.setCurationsSettingsId(curationsSettingsId);

            CurationSettings curationSettings = new CurationSettings(
                randomDocReferenceList(),
                randomDocReferenceList(),
                randomConditionList()
            );

            try {
                when(curationsService.getCurationsSettings(eq(curationsSettingsId))).thenReturn(curationSettings);
                when(curationsService.getCurationsSettings(not(eq(curationsSettingsId)))).thenThrow(
                    new CurationsService.CurationsSettingsNotFoundException("Curations settings not found")
                );
            } catch (CurationsService.CurationsSettingsNotFoundException e) {
                // Can't happen defining mock
            }
        }

        QueryFieldsResolver queryFieldsResolver = mock(QueryFieldsResolver.class);
        when(queryFieldsResolver.getQueryFields(any())).thenReturn(randomSet(0, 10, () -> randomString()));

        builder.setRelevanceSettingsService(relevanceSettingsService);
        builder.setCurationsService(curationsService);

        return builder;
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

        rewritten.setQueryFieldsResolver(queryBuilder.getQueryFieldsResolver());
        rewritten.setRelevanceSettingsService(queryBuilder.getRelevanceSettingsService());
        rewritten.setCurationsService(queryBuilder.getCurationsService());

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
         */
    }

    private static List<Condition> randomConditionList() {
        return randomList(0, 10, () -> new Condition.QueryCondition(randomString()));
    }

    private static List<CurationSettings.DocumentReference> randomDocReferenceList() {
        return randomList(0, 10, () -> new CurationSettings.DocumentReference(randomString(), randomString()));
    }

    private static String randomString() {
        return RandomStrings.randomAsciiLettersOfLength(random(), 10);
    }

}
