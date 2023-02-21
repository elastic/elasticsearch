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
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.Collections;

public class MultiplexerTokenFilterTests extends ESTokenStreamTestCase {

    public void testMultiplexingFilter() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.t.type", "truncate")
            .put("index.analysis.filter.t.length", "2")
            .put("index.analysis.filter.multiplexFilter.type", "multiplexer")
            .putList("index.analysis.filter.multiplexFilter.filters", "lowercase, t", "uppercase")
            .put("index.analysis.analyzer.myAnalyzer.type", "custom")
            .put("index.analysis.analyzer.myAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.myAnalyzer.filter", "multiplexFilter")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        IndexAnalyzers indexAnalyzers = new AnalysisModule(
            TestEnvironment.newEnvironment(settings),
            Collections.singletonList(new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry().build(idxSettings);

        try (NamedAnalyzer analyzer = indexAnalyzers.get("myAnalyzer")) {
            assertNotNull(analyzer);
            assertAnalyzesTo(
                analyzer,
                "ONe tHree",
                new String[] { "ONe", "on", "ONE", "tHree", "th", "THREE" },
                new int[] { 1, 0, 0, 1, 0, 0 }
            );
            // Duplicates are removed
            assertAnalyzesTo(analyzer, "ONe THREE", new String[] { "ONe", "on", "ONE", "THREE", "th" }, new int[] { 1, 0, 0, 1, 0, 0 });
        }
    }

    public void testMultiplexingNoOriginal() throws IOException {

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.t.type", "truncate")
            .put("index.analysis.filter.t.length", "2")
            .put("index.analysis.filter.multiplexFilter.type", "multiplexer")
            .put("index.analysis.filter.multiplexFilter.preserve_original", "false")
            .putList("index.analysis.filter.multiplexFilter.filters", "lowercase, t", "uppercase")
            .put("index.analysis.analyzer.myAnalyzer.type", "custom")
            .put("index.analysis.analyzer.myAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.myAnalyzer.filter", "multiplexFilter")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        IndexAnalyzers indexAnalyzers = new AnalysisModule(
            TestEnvironment.newEnvironment(settings),
            Collections.singletonList(new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry().build(idxSettings);

        try (NamedAnalyzer analyzer = indexAnalyzers.get("myAnalyzer")) {
            assertNotNull(analyzer);
            assertAnalyzesTo(analyzer, "ONe tHree", new String[] { "on", "ONE", "th", "THREE" }, new int[] { 1, 0, 1, 0, });
        }

    }

}
