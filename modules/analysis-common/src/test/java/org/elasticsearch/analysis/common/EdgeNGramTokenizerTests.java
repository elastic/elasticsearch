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

package org.elasticsearch.analysis.common;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collections;

public class EdgeNGramTokenizerTests extends ESTokenStreamTestCase {

    public void testPreConfiguredTokenizer() throws IOException {

        // Before 7.3 we return ngrams of length 1 only
        {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED,
                    VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, VersionUtils.getPreviousVersion(Version.V_7_3_0)))
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "edge_ngram")
                .build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

            try (IndexAnalyzers indexAnalyzers = new AnalysisModule(TestEnvironment.newEnvironment(settings),
                Collections.singletonList(new CommonAnalysisPlugin())).getAnalysisRegistry().build(idxSettings)) {

                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[]{"t"});

            }
        }

        // Check deprecated name as well
        {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED,
                    VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, VersionUtils.getPreviousVersion(Version.V_7_3_0)))
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "edgeNGram")
                .build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

            try (IndexAnalyzers indexAnalyzers = new AnalysisModule(TestEnvironment.newEnvironment(settings),
                Collections.singletonList(new CommonAnalysisPlugin())).getAnalysisRegistry().build(idxSettings)) {

                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[]{"t"});

            }
        }

        // Afterwards, we return ngrams of length 1 and 2, to match the default factory settings
        {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "edge_ngram")
                .build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

            try (IndexAnalyzers indexAnalyzers = new AnalysisModule(TestEnvironment.newEnvironment(settings),
                Collections.singletonList(new CommonAnalysisPlugin())).getAnalysisRegistry().build(idxSettings)) {

                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[]{"t", "te"});

            }
        }

        // Check deprecated name as well
        {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "edgeNGram")
                .build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

            try (IndexAnalyzers indexAnalyzers = new AnalysisModule(TestEnvironment.newEnvironment(settings),
                Collections.singletonList(new CommonAnalysisPlugin())).getAnalysisRegistry().build(idxSettings)) {

                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[]{"t", "te"});

            }
        }

    }

}
