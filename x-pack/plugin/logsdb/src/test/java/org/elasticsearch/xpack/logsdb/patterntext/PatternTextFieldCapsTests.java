/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PatternTextFieldCapsTests extends ESSingleNodeTestCase {

    private static final String INDEX = "test_index";

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MapperExtrasPlugin.class, XPackPlugin.class, LogsDBPlugin.class);
    }

    /**
     * pattern_text hard-codes its analyzers, so redefining {@code standard} under index.analysis does not change them.
     * Field caps withhold their names, which no node can rebuild, without flagging them as index-local.
     */
    public void testHardCodedAnalyzerIsWithheldButNotIndexLocal() {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.standard.type", "custom")
            .put("index.analysis.analyzer.standard.tokenizer", "standard")
            .build();
        String mapping = """
            {
              "properties": {
                "message": { "type": "pattern_text", "analyzer": "standard" },
                "delimited": { "type": "pattern_text" },
                "body": { "type": "text", "analyzer": "standard" }
              }
            }
            """;
        createIndex(INDEX, indicesAdmin().prepareCreate(INDEX).setSettings(settings).setMapping(mapping));

        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().indices(INDEX).fields("*");
        request.setMergeResults(false);
        FieldCapabilitiesResponse response = client().execute(TransportFieldCapabilitiesAction.TYPE, request).actionGet();
        Map<String, IndexFieldCapabilities> caps = response.getIndexResponses().getFirst().get();

        for (String field : List.of("message", "delimited")) {
            assertNull(field, caps.get(field).indexAnalyzer());
            assertFalse(field, caps.get(field).indexLocalAnalyzer());
        }
        assertNull(caps.get("body").indexAnalyzer());
        assertTrue(caps.get("body").indexLocalAnalyzer());
    }
}
