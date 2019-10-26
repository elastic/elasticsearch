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

package org.elasticsearch.index.store;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.transport.MockTransportService;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration test for corrupted translog files
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class CorruptedTranslogIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class);
    }

    public void testCorruptTranslogFiles() throws Exception {
        internalCluster().startNode(Settings.EMPTY);

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", "-1")
            .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true) // never flush - always recover from translog
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB))));

        // Index some documents
        IndexRequestBuilder[] builders = new IndexRequestBuilder[scaledRandomIntBetween(100, 1000)];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test").setSource("foo", "bar");
        }

        indexRandom(false, false, false, Arrays.asList(builders));

        final Path translogPath = internalCluster().getInstance(IndicesService.class)
            .indexService(resolveIndex("test")).getShard(0).shardPath().resolveTranslog();

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback(){
            @Override
            public void onAllNodesStopped() throws Exception {
                TestTranslog.corruptRandomTranslogFile(logger, random(), translogPath);
            }
        });

        assertBusy(() -> {
            final ClusterAllocationExplainResponse allocationExplainResponse
                = client().admin().cluster().prepareAllocationExplain().setIndex("test").setShard(0).setPrimary(true).get();
            final UnassignedInfo unassignedInfo = allocationExplainResponse.getExplanation().getUnassignedInfo();
            assertThat(unassignedInfo, not(nullValue()));
            final Throwable cause = ExceptionsHelper.unwrap(unassignedInfo.getFailure(), TranslogCorruptedException.class);
            assertThat(cause, not(nullValue()));
            assertThat(cause.getMessage(), containsString(translogPath.toString()));
        });

        assertThat(expectThrows(SearchPhaseExecutionException.class, () -> client().prepareSearch("test").setQuery(matchAllQuery()).get())
            .getMessage(), containsString("all shards failed"));

    }

}
