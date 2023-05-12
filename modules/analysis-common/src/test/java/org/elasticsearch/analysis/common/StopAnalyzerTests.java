/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import static org.elasticsearch.test.ESTestCase.createTestAnalysis;

public class StopAnalyzerTests extends ESTokenStreamTestCase {
    public void testDefaultsCompoundAnalysis() throws Exception {
        String json = "/org/elasticsearch/analysis/common/stop.json";
        Settings settings = Settings.builder()
            .loadFromStream(json, getClass().getResourceAsStream(json), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        IndexAnalyzers indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
        NamedAnalyzer analyzer1 = indexAnalyzers.get("analyzer1");

        assertTokenStreamContents(analyzer1.tokenStream("test", "to be or not to be"), new String[0]);

        NamedAnalyzer analyzer2 = indexAnalyzers.get("analyzer2");

        assertTokenStreamContents(analyzer2.tokenStream("test", "to be or not to be"), new String[0]);
    }
}
