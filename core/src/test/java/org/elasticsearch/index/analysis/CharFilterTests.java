/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.analysis;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

/**
 */
public class CharFilterTests extends ESTokenStreamTestCase {
    public void testMappingCharFilter() throws Exception {
        Settings settings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.analysis.char_filter.my_mapping.type", "mapping")
                .putArray("index.analysis.char_filter.my_mapping.mappings", "ph=>f", "qu=>q")
                .put("index.analysis.analyzer.custom_with_char_filter.tokenizer", "standard")
                .putArray("index.analysis.analyzer.custom_with_char_filter.char_filter", "my_mapping")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        AnalysisService analysisService = new AnalysisRegistry(null, new Environment(settings)).build(idxSettings);
        NamedAnalyzer analyzer1 = analysisService.analyzer("custom_with_char_filter");

        assertTokenStreamContents(analyzer1.tokenStream("test", "jeff quit phish"), new String[]{"jeff", "qit", "fish"});

        // Repeat one more time to make sure that char filter is reinitialized correctly
        assertTokenStreamContents(analyzer1.tokenStream("test", "jeff quit phish"), new String[]{"jeff", "qit", "fish"});
    }

    public void testHtmlStripCharFilter() throws Exception {
        Settings settings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.analysis.analyzer.custom_with_char_filter.tokenizer", "standard")
                .putArray("index.analysis.analyzer.custom_with_char_filter.char_filter", "html_strip")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        AnalysisService analysisService = new AnalysisRegistry(null, new Environment(settings)).build(idxSettings);

        NamedAnalyzer analyzer1 = analysisService.analyzer("custom_with_char_filter");

        assertTokenStreamContents(analyzer1.tokenStream("test", "<b>hello</b>!"), new String[]{"hello"});

        // Repeat one more time to make sure that char filter is reinitialized correctly
        assertTokenStreamContents(analyzer1.tokenStream("test", "<b>hello</b>!"), new String[]{"hello"});
    }

    public void testPatternReplaceCharFilter() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.char_filter.my_mapping.type", "pattern_replace")
            .put("index.analysis.char_filter.my_mapping.pattern", "ab*")
            .put("index.analysis.char_filter.my_mapping.replacement", "oo")
            .put("index.analysis.char_filter.my_mapping.flags", "CASE_INSENSITIVE")
            .put("index.analysis.analyzer.custom_with_char_filter.tokenizer", "standard")
            .putArray("index.analysis.analyzer.custom_with_char_filter.char_filter", "my_mapping")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        AnalysisService analysisService = new AnalysisRegistry(null, new Environment(settings)).build(idxSettings);
        NamedAnalyzer analyzer1 = analysisService.analyzer("custom_with_char_filter");

        assertTokenStreamContents(analyzer1.tokenStream("test", "faBBbBB aBbbbBf"), new String[]{"foo", "oof"});
    }
}
