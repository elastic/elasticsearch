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

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.util.settings.Settings;
import org.testng.annotations.Test;

import static org.elasticsearch.util.settings.ImmutableSettings.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class AnalysisModuleTests {

    @Test public void testSimpleConfigurationJson() {
        Settings settings = settingsBuilder().loadFromClasspath("org/elasticsearch/index/analysis/test1.json").build();
        testSimpleConfiguration(settings);
    }

    @Test public void testSimpleConfigurationYaml() {
        Settings settings = settingsBuilder().loadFromClasspath("org/elasticsearch/index/analysis/test1.yml").build();
        testSimpleConfiguration(settings);
    }

    private void testSimpleConfiguration(Settings settings) {
        Index index = new Index("test");
        Injector injector = Guice.createInjector(
                new IndexSettingsModule(settings),
                new IndexNameModule(index),
                new AnalysisModule(settings));

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);

        Analyzer analyzer = analysisService.analyzer("custom1");

        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom1 = (CustomAnalyzer) analyzer;
        assertThat(custom1.tokenizerFactory(), instanceOf(StandardTokenizerFactory.class));
        assertThat(custom1.tokenFilters().length, equalTo(2));

        StopTokenFilterFactory stop1 = (StopTokenFilterFactory) custom1.tokenFilters()[0];
        assertThat(stop1.stopWords().size(), equalTo(1));
        assertThat(stop1.stopWords(), hasItem("test-stop"));
    }
}
