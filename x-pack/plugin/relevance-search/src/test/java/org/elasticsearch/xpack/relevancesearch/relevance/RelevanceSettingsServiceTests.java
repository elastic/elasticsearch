/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.FunctionalBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ProximityBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ScriptScoreBoost;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ValueBoost;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelevanceSettingsServiceTests extends ESTestCase {
    private Client clientWithSource(String source) {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        GetResult result = new GetResult(".enterprise-search", "1", 0, 1, 1, true, new BytesArray(source), emptyMap(), emptyMap());
        GetResponse response = new GetResponse(result);
        GetRequestBuilder builder = mock(GetRequestBuilder.class);
        when(builder.get()).thenReturn(response);
        when(client.prepareGet(anyString(), anyString())).thenReturn(builder);
        return client;
    }

    public void testParseFields() throws RelevanceSettingsService.RelevanceSettingsInvalidException,
        RelevanceSettingsService.RelevanceSettingsNotFoundException {
        String source = """
            {
              "query_configuration": {
                "fields": ["title^3","description^2"]
              }
            }""";
        Client client = this.clientWithSource(source);
        final RelevanceSettingsService service = new RelevanceSettingsService(client);
        RelevanceSettings actual = service.getRelevanceSettings("settings-id-here");
        Map<String, Float> expected = Map.of("title", 3f, "description", 2f);
        assertEquals(expected, actual.getQueryConfiguration().getFieldsAndBoosts());
    }

    public void testParseValueBoost() throws RelevanceSettingsService.RelevanceSettingsInvalidException,
        RelevanceSettingsService.RelevanceSettingsNotFoundException {
        String source = """
            {
              "query_configuration": {
                "fields": ["title","description"],
                "boosts": {
                  "world_heritage_site": [
                    {
                      "type": "value",
                      "value": "true",
                      "operation": "multiply",
                      "factor": 10
                    }
                  ]
                }
              }
            }""";
        Client client = this.clientWithSource(source);
        final RelevanceSettingsService service = new RelevanceSettingsService(client);
        RelevanceSettings settings = service.getRelevanceSettings("settings-id-here");
        Map<String, List<ScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<ScriptScoreBoost>> expected = Collections.singletonMap(
            "world_heritage_site",
            Collections.singletonList(new ValueBoost("true", "multiply", 10f))
        );
        assertEquals(expected, actual);
    }

    public void testParseFunctionalBoost() throws RelevanceSettingsService.RelevanceSettingsInvalidException,
        RelevanceSettingsService.RelevanceSettingsNotFoundException {
        String source = """
            {
              "query_configuration": {
                "fields": ["title","description", "visitors"],
                "boosts": {
                  "visitors": [
                    {
                      "type": "functional",
                      "function": "linear",
                      "operation": "add",
                      "factor": 5
                    }
                  ]
                }
              }
            }""";
        Client client = this.clientWithSource(source);
        final RelevanceSettingsService service = new RelevanceSettingsService(client);
        RelevanceSettings settings = service.getRelevanceSettings("settings-id-here");
        Map<String, List<ScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<ScriptScoreBoost>> expected = Collections.singletonMap(
            "visitors",
            Collections.singletonList(new FunctionalBoost("linear", "add", 5f))
        );
        assertEquals(expected, actual);
    }

    public void testParseProximityBoost() throws RelevanceSettingsService.RelevanceSettingsInvalidException,
        RelevanceSettingsService.RelevanceSettingsNotFoundException {
        String source = """
            {
              "query_configuration": {
                "fields": ["title","description", "location"],
                "boosts": {
                  "location": [
                    {
                      "type": "proximity",
                      "center": "25.32, -80.93",
                      "function": "gaussian",
                      "factor": 5
                    }
                  ]
                }
              }
            }""";
        Client client = this.clientWithSource(source);
        final RelevanceSettingsService service = new RelevanceSettingsService(client);
        RelevanceSettings settings = service.getRelevanceSettings("settings-id-here");
        Map<String, List<ScriptScoreBoost>> actual = settings.getQueryConfiguration().getScriptScores();

        Map<String, List<ScriptScoreBoost>> expected = Collections.singletonMap(
            "location",
            Collections.singletonList(new ProximityBoost("25.32, -80.93", "gaussian", 5f))
        );
        assertEquals(expected, actual);
    }
}
