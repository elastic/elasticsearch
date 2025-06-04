/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.exception.ExceptionsHelper;
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

import static org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanationUtils.getClusterAllocationExplanation;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class CorruptedTranslogIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class);
    }

    public void testCorruptTranslogFiles() throws Exception {
        internalCluster().startNode(Settings.EMPTY);

        assertAcked(
            prepareCreate("test").setSettings(
                indexSettings(1, 0).put("index.refresh_interval", "-1")
                    .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true) // never flush - always recover from translog
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.PB))
            )
        );

        IndexRequestBuilder[] builders = new IndexRequestBuilder[scaledRandomIntBetween(100, 1000)];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex("test").setSource("foo", "bar");
        }

        indexRandom(false, false, false, Arrays.asList(builders));

        final Path translogPath = internalCluster().getInstance(IndicesService.class)
            .indexService(resolveIndex("test"))
            .getShard(0)
            .shardPath()
            .resolveTranslog();

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public void onAllNodesStopped() throws Exception {
                TestTranslog.corruptRandomTranslogFile(logger, random(), translogPath);
            }
        });

        assertBusy(() -> {
            // assertBusy since the shard starts out unassigned with reason CLUSTER_RECOVERED, then it's assigned, and then it fails.
            final var allocationExplainResponse = getClusterAllocationExplanation(client(), "test", 0, true);
            final var description = Strings.toString(allocationExplainResponse);
            final var unassignedInfo = allocationExplainResponse.getUnassignedInfo();
            assertThat(description, unassignedInfo, not(nullValue()));
            assertThat(description, unassignedInfo.reason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
            var failure = unassignedInfo.failure();
            assertNotNull(failure);
            final Throwable cause = ExceptionsHelper.unwrap(failure, TranslogCorruptedException.class);
            if (cause != null) {
                assertThat(description, cause.getMessage(), containsString(translogPath.toString()));
            }
        });

        assertThat(
            expectThrows(SearchPhaseExecutionException.class, prepareSearch("test").setQuery(matchAllQuery())).getMessage(),
            containsString("all shards failed")
        );

    }

}
