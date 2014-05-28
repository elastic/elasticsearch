/*
 * Licensed to Elasticsearch (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
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

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 1, scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class SimplePhoneticIntegrationTests extends ElasticsearchIntegrationTest {

    @Override
    public Settings indexSettings() {
        Settings settings = ImmutableSettings.builder()
                .put(super.indexSettings())
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                .putArray("index.analysis.analyzer.my_analyzer.filter", "standard", "lowercase", "my_metaphone")
                .put("index.analysis.filter.my_metaphone.type", "phonetic")
                .put("index.analysis.filter.my_metaphone.encoder", "metaphone")
                .put("index.analysis.filter.my_metaphone.replace", false)
                .build();

        return settings;
    }

    @Test
    public void testPhoneticAnalyzer() throws ExecutionException, InterruptedException {
        createIndex("test");
        ensureGreen("test");
        AnalyzeResponse response = client().admin().indices()
                .prepareAnalyze("hello world")
                .setIndex("test")
                .setAnalyzer("my_analyzer")
                .execute().get();

        assertThat(response, notNullValue());
        assertThat(response.getTokens().size(), is(4));
        assertThat(response.getTokens().get(0).getTerm(), is("HL"));
        assertThat(response.getTokens().get(1).getTerm(), is("hello"));
        assertThat(response.getTokens().get(2).getTerm(), is("WRLT"));
        assertThat(response.getTokens().get(3).getTerm(), is("world"));
    }

    @Test
    public void testPhoneticAnalyzerInMapping() throws ExecutionException, InterruptedException, IOException {
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

        index("test", "type", "1", "foo", "hello world");
        refresh();

        SearchResponse response = client().prepareSearch("test").setQuery(
                QueryBuilders.matchQuery("foo", "helllo")
        ).execute().actionGet();

        assertThat(response.getHits().getTotalHits(), is(1L));
    }

}
