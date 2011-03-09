/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.lucene.all.AllTokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.compound.DictionaryCompoundWordTokenFilterFactory;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.hamcrest.MatcherAssert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.hamcrest.Matchers.*;

/**
 * @author Edward Dale (scompt@scompt.com)
 */
public class CompoundAnalysisTests {

    @Test public void testDefaultsCompoundAnalysis() throws Exception {
        Index index = new Index("test");
        Settings settings = getJsonSettings();

        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings)).createInjector();

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        TokenFilterFactory filterFactory = analysisService.tokenFilter("dict_dec");
        MatcherAssert.assertThat(filterFactory, instanceOf(DictionaryCompoundWordTokenFilterFactory.class));
    }

    @Test public void testDictionaryDecompounder() throws Exception {
        Settings[] settingsArr = new Settings[]{getJsonSettings(), getYamlSettings()};
        for (Settings settings : settingsArr) {
            List<String> terms = analyze(settings, "decompoundingAnalyzer", "donaudampfschiff spargelcremesuppe");
            MatcherAssert.assertThat(terms.size(), equalTo(8));
            MatcherAssert.assertThat(terms, hasItems("donau", "dampf", "schiff", "donaudampfschiff", "spargel", "creme", "suppe", "spargelcremesuppe"));
        }
    }

    private List<String> analyze(Settings settings, String analyzerName, String text) throws IOException {
        Index index = new Index("test");
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings)).createInjector();

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        Analyzer analyzer = analysisService.analyzer(analyzerName).analyzer();

        AllEntries allEntries = new AllEntries();
        allEntries.addText("field1", text, 1.0f);
        allEntries.reset();

        TokenStream stream = AllTokenStream.allTokenStream("_all", allEntries, analyzer);
        TermAttribute termAtt = stream.addAttribute(TermAttribute.class);

        List<String> terms = new ArrayList<String>();
        while (stream.incrementToken()) {
            String tokText = termAtt.term();
            terms.add(tokText);
        }
        return terms;
    }

    private Settings getJsonSettings() {
        return settingsBuilder().loadFromClasspath("org/elasticsearch/index/analysis/test1.json").build();
    }

    private Settings getYamlSettings() {
        return settingsBuilder().loadFromClasspath("org/elasticsearch/index/analysis/test1.yml").build();
    }
}
