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

package org.elasticsearch.index.analysis.synonyms;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.lucene.all.AllTokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class SynonymsAnalysisTest extends ElasticsearchTestCase {

    protected final ESLogger logger = Loggers.getLogger(getClass());
    private AnalysisService analysisService;

    @Test
    public void testSynonymsAnalysis() throws IOException {

        Settings settings = settingsBuilder().
                loadFromClasspath("org/elasticsearch/index/analysis/synonyms/synonyms.json")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();

        Index index = new Index("test");

        Injector parentInjector = new ModulesBuilder().add(
                new SettingsModule(settings),
                new EnvironmentModule(new Environment(settings)),
                new IndicesAnalysisModule())
                .createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class)))
                .createChildInjector(parentInjector);

        analysisService = injector.getInstance(AnalysisService.class);

        match("synonymAnalyzer", "kimchy is the dude abides", "shay is the elasticsearch man!");
        match("synonymAnalyzer_file", "kimchy is the dude abides", "shay is the elasticsearch man!");
        match("synonymAnalyzerWordnet", "abstain", "abstain refrain desist");
        match("synonymAnalyzerWordnet_file", "abstain", "abstain refrain desist");
        match("synonymAnalyzerWithsettings", "kimchy", "sha hay");

    }

    private void match(String analyzerName, String source, String target) throws IOException {

        Analyzer analyzer = analysisService.analyzer(analyzerName).analyzer();

        AllEntries allEntries = new AllEntries();
        allEntries.addText("field", source, 1.0f);
        allEntries.reset();

        TokenStream stream = AllTokenStream.allTokenStream("_all", allEntries, analyzer);
        stream.reset();
        CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);

        StringBuilder sb = new StringBuilder();
        while (stream.incrementToken()) {
            sb.append(termAtt.toString()).append(" ");
        }

        MatcherAssert.assertThat(target, equalTo(sb.toString().trim()));
    }

}
