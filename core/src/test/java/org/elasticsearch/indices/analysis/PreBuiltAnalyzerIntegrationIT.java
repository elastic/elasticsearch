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

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESBackcompatTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
@ESBackcompatTestCase.CompatibilityVersion(version = Version.V_1_2_0_ID) // we throw an exception if we create an index with _field_names that is 1.3
public class PreBuiltAnalyzerIntegrationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(DummyAnalysisPlugin.class);
    }

    @Test
    public void testThatPreBuiltAnalyzersAreNotClosedOnIndexClose() throws Exception {
        Map<PreBuiltAnalyzers, List<Version>> loadedAnalyzers = new HashMap<>();
        List<String> indexNames = new ArrayList<>();
        final int numIndices = scaledRandomIntBetween(2, 4);
        for (int i = 0; i < numIndices; i++) {
            String indexName = randomAsciiOfLength(10).toLowerCase(Locale.ROOT);
            indexNames.add(indexName);

            int randomInt = randomInt(PreBuiltAnalyzers.values().length-1);
            PreBuiltAnalyzers preBuiltAnalyzer = PreBuiltAnalyzers.values()[randomInt];
            String name = preBuiltAnalyzer.name().toLowerCase(Locale.ROOT);

            Version randomVersion = randomVersion(random());
            if (!loadedAnalyzers.containsKey(preBuiltAnalyzer)) {
                 loadedAnalyzers.put(preBuiltAnalyzer, new ArrayList<Version>());
            }
            loadedAnalyzers.get(preBuiltAnalyzer).add(randomVersion);

            final XContentBuilder mapping = jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "string")
                            .field("analyzer", name)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject();

            Settings versionSettings = settings(randomVersion).build();
            client().admin().indices().prepareCreate(indexName).addMapping("type", mapping).setSettings(versionSettings).get();
        }

        ensureGreen();

        final int numDocs = randomIntBetween(10, 100);
        // index some amount of data
        for (int i = 0; i < numDocs; i++) {
            String randomIndex = indexNames.get(randomInt(indexNames.size()-1));
            String randomId = randomInt() + "";

            Map<String, Object> data = new HashMap<>();
            data.put("foo", randomAsciiOfLength(scaledRandomIntBetween(5, 50)));

            index(randomIndex, "type", randomId, data);
        }

        refresh();

        // close some of the indices
        int amountOfIndicesToClose = randomInt(numIndices-1);
        for (int i = 0; i < amountOfIndicesToClose; i++) {
            String indexName = indexNames.get(i);
            client().admin().indices().prepareClose(indexName).execute().actionGet();
        }

        ensureGreen();

        // check that all above configured analyzers have been loaded
        assertThatAnalyzersHaveBeenLoaded(loadedAnalyzers);

        // check that all of the prebuiltanalyzers are still open
        assertLuceneAnalyzersAreNotClosed(loadedAnalyzers);
    }

    /**
     * Test case for #5030: Upgrading analysis plugins fails
     * See https://github.com/elasticsearch/elasticsearch/issues/5030
     */
    @Test
    public void testThatPluginAnalyzersCanBeUpdated() throws Exception {
        final XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", "string")
                        .field("analyzer", "dummy")
                    .endObject()
                    .startObject("bar")
                        .field("type", "string")
                        .field("analyzer", "my_dummy")
                    .endObject()
                .endObject()
            .endObject()
            .endObject();

        Settings versionSettings = settings(randomVersion(random()))
                .put("index.analysis.analyzer.my_dummy.type", "custom")
                .put("index.analysis.analyzer.my_dummy.filter", "my_dummy_token_filter")
                .put("index.analysis.analyzer.my_dummy.char_filter", "my_dummy_char_filter")
                .put("index.analysis.analyzer.my_dummy.tokenizer", "my_dummy_tokenizer")
                .put("index.analysis.tokenizer.my_dummy_tokenizer.type", "dummy_tokenizer")
                .put("index.analysis.filter.my_dummy_token_filter.type", "dummy_token_filter")
                .put("index.analysis.char_filter.my_dummy_char_filter.type", "dummy_char_filter")
                .build();

        client().admin().indices().prepareCreate("test-analysis-dummy").addMapping("type", mapping).setSettings(versionSettings).get();

        ensureGreen();
    }

    private void assertThatAnalyzersHaveBeenLoaded(Map<PreBuiltAnalyzers, List<Version>> expectedLoadedAnalyzers) {
        for (Map.Entry<PreBuiltAnalyzers, List<Version>> entry : expectedLoadedAnalyzers.entrySet()) {
            for (Version version : entry.getValue()) {
                // if it is not null in the cache, it has been loaded
                assertThat(entry.getKey().getCache().get(version), is(notNullValue()));
            }
        }
    }

    // ensure analyzers are still open by checking there is no ACE
    private void assertLuceneAnalyzersAreNotClosed(Map<PreBuiltAnalyzers, List<Version>> loadedAnalyzers) throws IOException {
        for (Map.Entry<PreBuiltAnalyzers, List<Version>> preBuiltAnalyzerEntry : loadedAnalyzers.entrySet()) {
            for (Version version : preBuiltAnalyzerEntry.getValue()) {
                Analyzer analyzer = preBuiltAnalyzerEntry.getKey().getCache().get(version);
                try (TokenStream stream = analyzer.tokenStream("foo", "bar")) {
                    stream.reset();
                    while (stream.incrementToken()) {
                    }
                    stream.end();
                }
            }
        }
    }
}
