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
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.junit.Test;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 */
public class CharFilterTests extends ESTokenStreamTestCase {

    @Test
    public void testMappingCharFilter() throws Exception {
        Index index = new Index("test");
        Settings settings = settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.analysis.char_filter.my_mapping.type", "mapping")
                .putArray("index.analysis.char_filter.my_mapping.mappings", "ph=>f", "qu=>q")
                .put("index.analysis.analyzer.custom_with_char_filter.tokenizer", "standard")
                .putArray("index.analysis.analyzer.custom_with_char_filter.char_filter", "my_mapping")
                .put("path.home", createTempDir().toString())
                .build();
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings), new EnvironmentModule(new Environment(settings))).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class)))
                .createChildInjector(parentInjector);

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        NamedAnalyzer analyzer1 = analysisService.analyzer("custom_with_char_filter");
        
        assertTokenStreamContents(analyzer1.tokenStream("test", "jeff quit phish"), new String[]{"jeff", "qit", "fish"});

        // Repeat one more time to make sure that char filter is reinitialized correctly
        assertTokenStreamContents(analyzer1.tokenStream("test", "jeff quit phish"), new String[]{"jeff", "qit", "fish"});
    }

    @Test
    public void testHtmlStripCharFilter() throws Exception {
        Index index = new Index("test");
        Settings settings = settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.analysis.analyzer.custom_with_char_filter.tokenizer", "standard")
                .putArray("index.analysis.analyzer.custom_with_char_filter.char_filter", "html_strip")
                .put("path.home", createTempDir().toString())
                .build();
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings), new EnvironmentModule(new Environment(settings))).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class)))
                .createChildInjector(parentInjector);

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        NamedAnalyzer analyzer1 = analysisService.analyzer("custom_with_char_filter");

        assertTokenStreamContents(analyzer1.tokenStream("test", "<b>hello</b>!"), new String[]{"hello"});

        // Repeat one more time to make sure that char filter is reinitialized correctly
        assertTokenStreamContents(analyzer1.tokenStream("test", "<b>hello</b>!"), new String[]{"hello"});
    }
}
