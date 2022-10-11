/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.FunctionalBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ProximityBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ScriptScoreBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ValueBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettings;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettingsService;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RelevanceSettingsServiceTests extends ESSingleNodeTestCase {

    private RelevanceSettingsService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        service = getInstanceFromNode(RelevanceSettingsService.class);
        createIndex(RelevanceSettingsService.ENT_SEARCH_INDEX);
    }

    public void testParseFields() throws Exception {
        String settingsId = "1";
        indexDocument(
            RelevanceSettingsService.ENT_SEARCH_INDEX,
            RelevanceSettingsService.RELEVANCE_SETTINGS_PREFIX + settingsId,
            Map.of("query_configuration", Map.of("fields", List.of("title^3", "description^2")))
        );
        RelevanceSettings settings = service.getRelevanceSettings(settingsId);
        Map<String, Float> expected = Map.of("title", 3f, "description", 2f);
        assertEquals(expected, settings.getQueryConfiguration().getFieldsAndBoosts());
    }

    public void testParseValueBoost() throws Exception {
        String settingsId = "1";
        indexDocument(
            RelevanceSettingsService.ENT_SEARCH_INDEX,
            RelevanceSettingsService.RELEVANCE_SETTINGS_PREFIX + settingsId,
            Map.of(
                "query_configuration",
                Map.of(
                    "fields",
                    List.of("title", "description"),
                    "boosts",
                    Map.of(
                        "world_heritage_site",
                        List.of(Map.of("type", "value", "operation", "multiply", "factor", "10", "value", "true"))
                    )
                )
            )
        );
        RelevanceSettings settings = service.getRelevanceSettings(settingsId);
        Map<String, List<ScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<ScriptScoreBoost>> expected = Collections.singletonMap(
            "world_heritage_site",
            Collections.singletonList(new ValueBoost("true", "multiply", 10f))
        );
        assertEquals(expected, actual);
    }

    public void testParseFunctionalBoost() throws Exception {
        String settingsId = "1";
        indexDocument(
            RelevanceSettingsService.ENT_SEARCH_INDEX,
            RelevanceSettingsService.RELEVANCE_SETTINGS_PREFIX + settingsId,
            Map.of(
                "query_configuration",
                Map.of(
                    "fields",
                    List.of("title", "description"),
                    "boosts",
                    Map.of("visitors", List.of(Map.of("type", "functional", "operation", "add", "factor", 5, "function", "linear")))
                )
            )
        );
        RelevanceSettings settings = service.getRelevanceSettings(settingsId);
        Map<String, List<ScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<ScriptScoreBoost>> expected = Collections.singletonMap(
            "visitors",
            Collections.singletonList(new FunctionalBoost("linear", "add", 5f))
        );
        assertEquals(expected, actual);
    }

    public void testParseProximityBoost() throws Exception {
        String settingsId = "1";
        indexDocument(
            RelevanceSettingsService.ENT_SEARCH_INDEX,
            RelevanceSettingsService.RELEVANCE_SETTINGS_PREFIX + settingsId,
            Map.of(
                "query_configuration",
                Map.of(
                    "fields",
                    List.of("title", "description"),
                    "boosts",
                    Map.of("location", List.of(Map.of("type", "proximity", "center", "25.32, -80.93", "factor", 5, "function", "gaussian")))
                )
            )
        );
        RelevanceSettings settings = service.getRelevanceSettings(settingsId);
        Map<String, List<ScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<ScriptScoreBoost>> expected = Collections.singletonMap(
            "location",
            Collections.singletonList(new ProximityBoost("25.32, -80.93", "gaussian", 5f))
        );
        assertEquals(expected, actual);
    }

    private void indexDocument(String index, String id, Map<String, Object> document) {
        client().prepareIndex(index)
            .setId(id)
            .setSource(document)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute()
            .actionGet();
    }
}
