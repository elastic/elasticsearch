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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;

public class WeightedAllFieldIntegrationTests extends ElasticsearchIntegrationTest {
    @Test
    public void termQueryFindsTopWeightedTerm() throws IOException, InterruptedException, ExecutionException {
        setup();
        SearchResponse response = client().prepareSearch("test").setQuery(termQuery("test", "test")).get();
        assertFirstHit(response, hasId("1"));
        assertSecondHit(response, hasId("2"));
    }

    @Test
    public void phraseQueryFindsTopWeightedPhrase() throws IOException, InterruptedException, ExecutionException {
        setup();
        SearchResponse response = client().prepareSearch("test").setQuery(matchPhraseQuery("test", "foo bar")).get();
        assertFirstHit(response, hasId("2"));
        assertSecondHit(response, hasId("1"));
    }

    private void setup() throws IOException, InterruptedException, ExecutionException {
        assertAcked(prepareCreate("test").setSettings(
                settingsBuilder().put(indexSettings()).put("analysis.analyzer.repeated.type", "custom")
                        .put("analysis.analyzer.repeated.tokenizer", "standard")
                        .putArray("analysis.analyzer.repeated.filter", "standard", "controlled_repeat")).addMapping(
                "test",
                jsonBuilder().startObject().startObject("test").startObject("properties").startObject("test")
                        .field("type", "string")
                        .field("index_analyzer", "repeated")
                        .field("search_analyzer", "standard")
                .endObject().endObject().endObject().endObject()));
        indexRandom(true, 
                client().prepareIndex("test", "test", "1").setSource("test", new String[] { "5 test", "1 foo bar" }),
                client().prepareIndex("test", "test", "2").setSource("test", new String[] { "5 foo bar", "1 test" }));
    }
}
