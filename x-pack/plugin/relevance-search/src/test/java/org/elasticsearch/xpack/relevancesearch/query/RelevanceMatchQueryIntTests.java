/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.relevancesearch.RelevanceSearchPlugin;
import org.elasticsearch.xpack.relevancesearch.relevance.RelevanceSettingsService;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;

public class RelevanceMatchQueryIntTests extends ESSingleNodeTestCase {

    private static final String DOCUMENTS_INDEX = "index";

    private RelevanceMatchQueryBuilder relevanceMatchQueryBuilder;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(RelevanceSearchPlugin.class);
    }

    @Before
    public void setup() {
        relevanceMatchQueryBuilder = new RelevanceMatchQueryBuilder();
        relevanceMatchQueryBuilder.setRelevanceSettingsService(getInstanceFromNode(RelevanceSettingsService.class));

        createIndex(DOCUMENTS_INDEX);
        createIndex(RelevanceSettingsService.ENT_SEARCH_INDEX);
    }

    public void testTextFieldsWitoutSettings() {
        indexDocument(
            DOCUMENTS_INDEX,
            "1",
            Map.of("textField", "text example", "intField", 12, "doubleField", 13.45, "anotherTextField", "should match")
        );
        indexDocument(
            DOCUMENTS_INDEX,
            "2",
            Map.of("textField", "other document", "intField", 12, "doubleField", 13.45, "anotherTextField", "should not be found"
            )
        );

        relevanceMatchQueryBuilder.setQuery("text match");
        SearchResponse response = client().prepareSearch(DOCUMENTS_INDEX).setQuery(relevanceMatchQueryBuilder).get();

        assertHitCount(response, 1);
        assertSearchHits(response, "1");
    }

    public void testNoTextFieldsWithoutSettings() {

        indexDocument(DOCUMENTS_INDEX, "2", Map.of("intField", 12, "doubleField", 13.45));

        relevanceMatchQueryBuilder.setQuery("text match");
        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(DOCUMENTS_INDEX).setQuery(relevanceMatchQueryBuilder);

        final String expectedMsg = "[relevance_match] query cannot find text fields in the index";
        assertFailures(searchRequestBuilder, RestStatus.BAD_REQUEST, containsString(expectedMsg));
    }

    public void testFieldSettings() {
        final String settingsId = "test-settings";
        indexDocument(
            RelevanceSettingsService.ENT_SEARCH_INDEX,
            RelevanceSettingsService.RELEVANCE_SETTINGS_PREFIX + settingsId,
            Map.of("fields", List.of("textField"))
        );

        indexDocument(
            DOCUMENTS_INDEX,
            "1",
            Map.of("textField", "text example", "intField", 12, "doubleField", 13.45, "anotherTextField", "should match")
        );
        indexDocument(
            DOCUMENTS_INDEX,
            "2",
            Map.of("textField", "other document", "intField", 12, "doubleField", 13.45, "anotherTextField", "text example")
        );

        relevanceMatchQueryBuilder.setQuery("text example");
        relevanceMatchQueryBuilder.setRelevanceSettingsId(settingsId);
        SearchResponse response = client().prepareSearch(DOCUMENTS_INDEX).setQuery(relevanceMatchQueryBuilder).get();

        assertHitCount(response, 1);
        assertSearchHits(response, "1");
    }

    public void testFieldSettingsNotFound() {
        relevanceMatchQueryBuilder.setQuery("text example");
        final String relevanceSettingsId = "non-existing-settings";
        relevanceMatchQueryBuilder.setRelevanceSettingsId(relevanceSettingsId);

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(DOCUMENTS_INDEX).setQuery(relevanceMatchQueryBuilder);

        final String expectedMsg = "[relevance_match] query can't find search settings: " + relevanceSettingsId;
        assertFailures(searchRequestBuilder, RestStatus.BAD_REQUEST, containsString(expectedMsg));
    }

    private void indexDocument(String index, String id, Map<String, Object> document) {
        client().prepareIndex(index)
            .setId(id)
            .setSource(document)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute()
            .actionGet();
        ;
    }
}
