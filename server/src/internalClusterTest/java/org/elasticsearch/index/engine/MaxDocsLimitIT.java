/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexWriterMaxDocsChanger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MaxDocsLimitIT extends ESIntegTestCase {

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {
        public static final int maxDocs = randomIntBetween(1, 20);

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> EngineTestCase.createEngine(config, maxDocs));
        }
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestEnginePlugin.class);
        return plugins;
    }

    public void testMaxDocsLimit() throws Exception {
        final int maxDocs = TestEnginePlugin.maxDocs;
        IndexWriterMaxDocsChanger.setMaxDocs(maxDocs);
        try {
            internalCluster().ensureAtLeastNumDataNodes(1);
            assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)));
            int numDocs = randomIntBetween(maxDocs + 1, maxDocs * 2);
            for (int i = 0; i < numDocs; i++) {
                if (i < maxDocs) {
                    final IndexResponse resp = client().prepareIndex("test").setSource("{}", XContentType.JSON).get();
                    assertThat(resp.status(), equalTo(RestStatus.CREATED));
                } else {
                    final IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
                        () -> client().prepareIndex("test").setSource("{}", XContentType.JSON).get());
                    assertThat(error.getMessage(), containsString("Number of documents in the index can't exceed [" + maxDocs + "]"));
                }
            }
            final IllegalArgumentException deleteError = expectThrows(IllegalArgumentException.class,
                () -> client().prepareDelete("test", "any-id").get());
            assertThat(deleteError.getMessage(), containsString("Number of documents in the index can't exceed [" + maxDocs + "]"));
            client().admin().indices().prepareRefresh("test").get();
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(new MatchAllQueryBuilder())
                .setTrackTotalHitsUpTo(Integer.MAX_VALUE).setSize(0).get();
            ElasticsearchAssertions.assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) maxDocs));
            if (randomBoolean()) {
                client().admin().indices().prepareFlush("test").get();
            }
            internalCluster().fullRestart();
            internalCluster().ensureAtLeastNumDataNodes(2);
            ensureGreen("test");
            searchResponse = client().prepareSearch("test").setQuery(new MatchAllQueryBuilder())
                .setTrackTotalHitsUpTo(Integer.MAX_VALUE).setSize(0).get();
            ElasticsearchAssertions.assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) maxDocs));
        } finally {
            IndexWriterMaxDocsChanger.restoreMaxDocs();
        }
    }
}
