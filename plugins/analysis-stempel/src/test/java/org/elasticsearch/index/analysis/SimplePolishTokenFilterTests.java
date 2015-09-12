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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
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
import org.elasticsearch.index.analysis.pl.PolishAnalysisBinderProcessor;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;

public class SimplePolishTokenFilterTests extends ESTestCase {

    @Test
    public void testBasicUsage() throws Exception {
        testToken("kwiaty", "kwć");
        testToken("canona", "ć");
        testToken("wirtualna", "wirtualny");
        testToken("polska", "polski");

        testAnalyzer("wirtualna polska", "wirtualny", "polski");
    }

    private void testToken(String source, String expected) throws IOException {
        Index index = new Index("test");
        Settings settings = Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("path.home", createTempDir())
                .put("index.analysis.filter.myStemmer.type", "polish_stem")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        TokenFilterFactory filterFactory = analysisService.tokenFilter("myStemmer");

        Tokenizer tokenizer = new KeywordTokenizer();
        tokenizer.setReader(new StringReader(source));
        TokenStream ts = filterFactory.create(tokenizer);

        CharTermAttribute term1 = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        assertThat(ts.incrementToken(), equalTo(true));

        assertThat(term1.toString(), equalTo(expected));
    }

    private void testAnalyzer(String source, String... expected_terms) throws IOException {
        Index index = new Index("test");
        Settings settings = Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("path.home", createTempDir())
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        Analyzer analyzer = analysisService.analyzer("polish").analyzer();

        TokenStream ts = analyzer.tokenStream("test", source);

        CharTermAttribute term1 = ts.addAttribute(CharTermAttribute.class);
        ts.reset();

        for (String expected : expected_terms) {
            assertThat(ts.incrementToken(), equalTo(true));
            assertThat(term1.toString(), equalTo(expected));
        }
    }

    private AnalysisService createAnalysisService(Index index, Settings settings) {
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings), new EnvironmentModule(new Environment(settings))).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class)).addProcessor(new PolishAnalysisBinderProcessor()))
                .createChildInjector(parentInjector);

        return injector.getInstance(AnalysisService.class);
    }
}
