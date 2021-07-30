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
import static org.hamcrest.Matchers.containsString;

public class PatternCaptureTokenFilterTests extends ESTokenStreamTestCase {
    public void testPatternCaptureTokenFilter() throws Exception {
        String json = "/org/elasticsearch/analysis/common/pattern_capture.json";
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .loadFromStream(json, getClass().getResourceAsStream(json), false)
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        IndexAnalyzers indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
        NamedAnalyzer analyzer1 = indexAnalyzers.get("single");

        assertTokenStreamContents(analyzer1.tokenStream("test", "foobarbaz"), new String[]{"foobarbaz","foobar","foo"});

        NamedAnalyzer analyzer2 = indexAnalyzers.get("multi");

        assertTokenStreamContents(analyzer2.tokenStream("test", "abc123def"), new String[]{"abc123def","abc","123","def"});

        NamedAnalyzer analyzer3 = indexAnalyzers.get("preserve");

        assertTokenStreamContents(analyzer3.tokenStream("test", "foobarbaz"), new String[]{"foobar","foo"});
    }

    public void testNoPatterns() {
        try {
            new PatternCaptureGroupTokenFilterFactory(IndexSettingsModule.newIndexSettings("test", Settings.EMPTY), null,
                    "pattern_capture", Settings.builder().put("pattern", "foobar").build());
            fail ("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("required setting 'patterns' is missing"));
        }
    }

}
