/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CombinedFieldsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.xpack.relevancesearch.relevance.QueryConfiguration;
import org.elasticsearch.xpack.relevancesearch.relevance.curations.Condition;
import org.elasticsearch.xpack.relevancesearch.relevance.curations.CurationSettings;
import org.elasticsearch.xpack.relevancesearch.relevance.curations.CurationsService;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettings;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettingsService;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelevanceMatchQueryRewriterTests extends AbstractBuilderTestCase {

    private static final String PINNED_DOC_1_ID = "pinnedDoc1";
    private static final String PINNED_DOC_2_ID = "pinnedDoc2";
    private static final String PINNED_INDEX = "index1";
    private static final String HIDDEN_DOC_1_ID = "hiddenDoc1";
    private static final String HIDDEN_DOC_2_ID = "hiddenDoc2";
    private static final String HIDDEN_INDEX = "index2";
    private static final String CURATION_SETTINGS_ID = "test-curation-settings";
    private static final String RELEVANCE_SETTINGS_ID = "test-relevance-settings";
    private static final String FIELD_1 = "field1";
    private static final String FIELD_2 = "field2";
    private static final String FIELD_3 = "field3";
    private static final String[] AVAILABLE_FIELDS = { FIELD_1, FIELD_2, FIELD_3 };
    private static final String[] RELEVANCE_SETTINGS_FIELDS = { FIELD_1, FIELD_3 };
    private RelevanceSettingsService relevanceSettingsService;
    private CurationsService curationsService;
    private QueryFieldsResolver queryFieldsResolver;

    private RelevanceMatchQueryBuilder queryBuilder;

    private RelevanceMatchQueryRewriter queryRewriter;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        queryBuilder = new RelevanceMatchQueryBuilder();

        relevanceSettingsService = mock(RelevanceSettingsService.class);
        curationsService = mock(CurationsService.class);
        queryFieldsResolver = mock(QueryFieldsResolver.class);

        queryRewriter = new RelevanceMatchQueryRewriter(relevanceSettingsService, curationsService, queryFieldsResolver);
    }

    public void testQueryWithoutRelevanceSettings() throws IOException {

        final String queryText = "query";
        queryBuilder.setQuery(queryText);

        SearchExecutionContext context = createSearchExecutionContext();
        when(queryFieldsResolver.getQueryFields(context)).thenReturn(Set.of(AVAILABLE_FIELDS));

        Query obtainedQuery = queryRewriter.rewriteQuery(queryBuilder, context);

        Query expectedQuery = QueryBuilders.combinedFieldsQuery(queryText, AVAILABLE_FIELDS).toQuery(context);

        assertEquals(expectedQuery, obtainedQuery);
    }

    public void testQueryWithRelevanceSettings() throws IOException {

        final String queryText = "query";
        queryBuilder.setQuery(queryText);

        setRelevanceSettings();

        SearchExecutionContext context = createSearchExecutionContext();
        Query obtainedQuery = queryRewriter.rewriteQuery(queryBuilder, context);

        Query expectedQuery = QueryBuilders.combinedFieldsQuery(queryText, RELEVANCE_SETTINGS_FIELDS).toQuery(context);

        assertEquals(expectedQuery, obtainedQuery);
    }

    public void testQueryWithCurationsPinnedAndHiddenDocs() throws IOException, CurationsService.CurationsSettingsNotFoundException {

        final String queryText = "matching";
        queryBuilder.setQuery(queryText);

        setRelevanceSettings();
        setCurationsSettings(true, true, queryText);

        SearchExecutionContext context = createSearchExecutionContext();
        Query obtainedQuery = queryRewriter.rewriteQuery(queryBuilder, context);

        Query expectedQuery = new BoolQueryBuilder().should(
            new PinnedQueryBuilder(
                new CombinedFieldsQueryBuilder(queryText, RELEVANCE_SETTINGS_FIELDS),
                new PinnedQueryBuilder.Item(PINNED_INDEX, PINNED_DOC_1_ID),
                new PinnedQueryBuilder.Item(PINNED_INDEX, PINNED_DOC_2_ID)
            )
        )
            .filter(
                new BoolQueryBuilder().mustNot(
                    new BoolQueryBuilder().must(new TermsQueryBuilder("_index", HIDDEN_INDEX))
                        .must(new TermsQueryBuilder("_id", HIDDEN_DOC_1_ID))
                )
                    .mustNot(
                        new BoolQueryBuilder().must(new TermsQueryBuilder("_index", HIDDEN_INDEX))
                            .must(new TermsQueryBuilder("_id", HIDDEN_DOC_2_ID))
                    )
            )
            .toQuery(context);

        assertEquals(expectedQuery, obtainedQuery);
    }

    public void testQueryWithCurationsNoHiddenDocs() throws IOException, CurationsService.CurationsSettingsNotFoundException {

        final String queryText = "matching";
        queryBuilder.setQuery(queryText);

        setRelevanceSettings();
        setCurationsSettings(true, false, queryText);

        SearchExecutionContext context = createSearchExecutionContext();
        Query obtainedQuery = queryRewriter.rewriteQuery(queryBuilder, context);

        Query expectedQuery = new PinnedQueryBuilder(
            new CombinedFieldsQueryBuilder(queryText, RELEVANCE_SETTINGS_FIELDS),
            new PinnedQueryBuilder.Item(PINNED_DOC_1_ID, PINNED_INDEX),
            new PinnedQueryBuilder.Item(PINNED_DOC_2_ID, PINNED_INDEX)
        ).toQuery(context);

        assertEquals(expectedQuery, obtainedQuery);
    }

    public void testQueryWithCurationsNoPinnedDocs() throws IOException, CurationsService.CurationsSettingsNotFoundException {

        final String queryText = "matching";
        queryBuilder.setQuery(queryText);

        setRelevanceSettings();
        setCurationsSettings(false, true, queryText);

        SearchExecutionContext context = createSearchExecutionContext();
        Query obtainedQuery = queryRewriter.rewriteQuery(queryBuilder, context);

        Query expectedQuery = new BoolQueryBuilder().should(new CombinedFieldsQueryBuilder(queryText, RELEVANCE_SETTINGS_FIELDS))
            .filter(
                new BoolQueryBuilder().mustNot(
                    new BoolQueryBuilder().must(new TermsQueryBuilder("_index", HIDDEN_INDEX))
                        .must(new TermsQueryBuilder("_id", HIDDEN_DOC_1_ID))
                )
                    .mustNot(
                        new BoolQueryBuilder().must(new TermsQueryBuilder("_index", HIDDEN_INDEX))
                            .must(new TermsQueryBuilder("_id", HIDDEN_DOC_2_ID))
                    )
            )
            .toQuery(context);

        assertEquals(expectedQuery, obtainedQuery);
    }

    public void testQueryWithCurationDoesNotMatchQuery() throws IOException, CurationsService.CurationsSettingsNotFoundException {

        final String queryText = "matching";
        queryBuilder.setQuery(queryText);

        setRelevanceSettings();
        setCurationsSettings(true, true, "query that will not match");

        SearchExecutionContext context = createSearchExecutionContext();
        Query obtainedQuery = queryRewriter.rewriteQuery(queryBuilder, context);

        Query expectedQuery = QueryBuilders.combinedFieldsQuery(queryText, RELEVANCE_SETTINGS_FIELDS).toQuery(context);

        assertEquals(expectedQuery, obtainedQuery);
    }

    private void setRelevanceSettings() {
        queryBuilder.setRelevanceSettingsId(RELEVANCE_SETTINGS_ID);

        RelevanceSettings relevanceSettings = new RelevanceSettings();
        QueryConfiguration queryConfiguration = new QueryConfiguration();
        queryConfiguration.setFieldsAndBoosts(Map.of(FIELD_1, 1.0f, FIELD_3, 1.0f));
        relevanceSettings.setQueryConfiguration(queryConfiguration);

        try {
            when(relevanceSettingsService.getRelevanceSettings(RELEVANCE_SETTINGS_ID)).thenReturn(relevanceSettings);
        } catch (RelevanceSettingsService.RelevanceSettingsNotFoundException
            | RelevanceSettingsService.RelevanceSettingsInvalidException e) {
            // Can't happen at mock definition
        }
    }

    private void setCurationsSettings(boolean withPinnedDocs, boolean withHiddenDocs, String queryToMatch)
        throws CurationsService.CurationsSettingsNotFoundException {

        queryBuilder.setCurationsSettingsId(CURATION_SETTINGS_ID);

        List<CurationSettings.DocumentReference> pinnedDocs = Collections.emptyList();
        if (withPinnedDocs) {
            pinnedDocs = List.of(
                new CurationSettings.DocumentReference(PINNED_DOC_1_ID, PINNED_INDEX),
                new CurationSettings.DocumentReference(PINNED_DOC_2_ID, PINNED_INDEX)
            );
        }

        List<CurationSettings.DocumentReference> hiddenDocs = Collections.emptyList();
        if (withHiddenDocs) {
            hiddenDocs = List.of(
                new CurationSettings.DocumentReference(HIDDEN_DOC_1_ID, HIDDEN_INDEX),
                new CurationSettings.DocumentReference(HIDDEN_DOC_2_ID, HIDDEN_INDEX)
            );
        }

        final List<Condition> conditions = List.of(
            new Condition.QueryCondition("non-matching query"),
            new Condition.QueryCondition(queryToMatch)
        );

        CurationSettings curationSettings = new CurationSettings(pinnedDocs, hiddenDocs, conditions);

        when(curationsService.getCurationsSettings(CURATION_SETTINGS_ID)).thenReturn(curationSettings);
    }

}
