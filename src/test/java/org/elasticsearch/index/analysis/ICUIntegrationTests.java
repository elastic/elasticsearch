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

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class ICUIntegrationTests extends ElasticsearchIntegrationTest {

    @Override
    public Settings indexSettings() {
        Settings settings = ImmutableSettings.builder()
                .put(super.indexSettings())
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                .putArray("index.analysis.analyzer.my_analyzer.filter", "standard", "lowercase", "my_collator")
                .put("index.analysis.filter.my_collator.type", "icu_collation")
                .put("index.analysis.filter.my_collator.language", "en")
                .put("index.analysis.filter.my_collator.strength", "primary")
                .build();

        return settings;
    }

    @Test
    public void testICUAnalyzer() throws ExecutionException, InterruptedException {
        createIndex("test");
        ensureGreen("test");
        AnalyzeResponse response1 = client().admin().indices()
                .prepareAnalyze("Bâton enflammé")
                .setIndex("test")
                .setAnalyzer("my_analyzer")
                .execute().get();
        AnalyzeResponse response2 = client().admin().indices()
                .prepareAnalyze("baton enflamme")
                .setIndex("test")
                .setAnalyzer("my_analyzer")
                .execute().get();

        assertThat(response1, notNullValue());
        assertThat(response2, notNullValue());
        assertThat(response1.getTokens().size(), is(response2.getTokens().size()));

        for (int i = 0; i < response2.getTokens().size(); i++) {
            assertThat(response1.getTokens().get(i).getTerm(), is(response2.getTokens().get(i).getTerm()));
        }
    }

    @Test
    public void testICUAnalyzerInMapping() throws ExecutionException, InterruptedException, IOException {
        createIndex("test");
        ensureGreen("test");
        final XContentBuilder mapping = jsonBuilder().startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("foo")
                .field("type", "string")
                .field("analyzer", "my_analyzer")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping).get();

        index("test", "type", "1", "foo", "Bâton enflammé");
        refresh();

        SearchResponse response = client().prepareSearch("test").setQuery(
                QueryBuilders.matchQuery("foo", "baton enflamme")
        ).execute().actionGet();

        assertThat(response.getHits().getTotalHits(), is(1L));
    }

    @Test
    public void testPluginIsLoaded() {
        NodesInfoResponse infos = client().admin().cluster().prepareNodesInfo().setPlugins(true).execute().actionGet();
        assertThat(infos.getNodes()[0].getPlugins().getInfos().get(0).getName(), is("analysis-icu"));
    }
}
