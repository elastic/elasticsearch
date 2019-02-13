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
package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.RetentionLeaseIT;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class PeerRecoveryRetentionLeaseIT extends ESIntegTestCase {

    public static final class RetentionLeaseSyncIntervalSettingPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(RetentionLeaseIT.RetentionLeaseSyncIntervalSettingPlugin.class))
            .collect(Collectors.toList());
    }

    @TestLogging("org.elasticsearch.indices.recovery:TRACE")
    public void testHistoryRetention() throws Exception {
        internalCluster().startNodes(3);

        final String indexName = "test";
        client().admin().indices().prepareCreate(indexName).setSettings(Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")).get();
        ensureGreen(indexName);

        // Perform some replicated operations so the replica isn't simply empty, because ops-based recovery isn't better in that case
        final List<IndexRequestBuilder> requests = new ArrayList<>();
        final int replicatedDocCount = scaledRandomIntBetween(25, 250);
        while (requests.size() < replicatedDocCount) {
            requests.add(client().prepareIndex(indexName, "_doc").setSource("{}", XContentType.JSON));
        }
        indexRandom(true, requests);
        if (randomBoolean()) {
            flush(indexName);
        }

        internalCluster().stopRandomNode(s -> true);
        internalCluster().stopRandomNode(s -> true);

        final long desyncNanoTime = System.nanoTime();
        while (System.nanoTime() <= desyncNanoTime) {
            // time passes
        }

        final int numNewDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numNewDocs; i++) {
            client().prepareIndex(indexName, "_doc").setSource("{}", XContentType.JSON).setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        }

        assertAcked(client().admin().indices().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)));
        internalCluster().startNode();
        ensureGreen(indexName);

        final RecoveryResponse recoveryResponse = client().admin().indices().recoveries(new RecoveryRequest(indexName)).get();
        final List<RecoveryState> recoveryStates = recoveryResponse.shardRecoveryStates().get(indexName);
        recoveryStates.removeIf(r -> r.getTimer().getStartNanoTime() <= desyncNanoTime);

        assertThat(recoveryStates, hasSize(1));
        assertThat(recoveryStates.get(0).getIndex().totalFileCount(), is(0));
        assertThat(recoveryStates.get(0).getTranslog().recoveredOperations(), greaterThan(0));
    }
}
