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
package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ShardStateActionIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        if (randomBoolean()) {
            builder.put(ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING.getKey(), randomPriority());
        }
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testFollowupRerouteAlwaysOccursEventually() {
        // Shows that no matter how cluster.routing.allocation.shard_state.reroute.priority is set, a follow-up reroute eventually occurs.
        // Can be removed when this setting is removed, as we copiously test the default case.

        internalCluster().ensureAtLeastNumDataNodes(2);

        if (randomBoolean()) {
            assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
                .put(ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING.getKey(), randomPriority())));
        }

        createIndex("test");
        final ClusterHealthResponse clusterHealthResponse
            = client().admin().cluster().prepareHealth().setWaitForNoInitializingShards(true).setWaitForEvents(Priority.LANGUID).get();
        assertFalse(clusterHealthResponse.isTimedOut());
        assertThat(clusterHealthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
            .putNull(ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING.getKey())));
    }

    public void testFollowupRerouteCanBeSetToHigherPriority() {
        // Shows that in a cluster under unbearable pressure we can still assign replicas (for now at least) by setting
        // cluster.routing.allocation.shard_state.reroute.priority to a higher priority. Can be removed when this setting is removed, as
        // we should at that point be confident that the default priority is appropriate for all clusters.

        internalCluster().ensureAtLeastNumDataNodes(2);

        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
            .put(ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING.getKey(), "urgent")));

        // ensure that the master always has a HIGH priority pending task
        final AtomicBoolean stopSpammingMaster = new AtomicBoolean();
        final ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        masterClusterService.submitStateUpdateTask("spam",
            new ClusterStateUpdateTask(Priority.HIGH) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError(source, e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    if (stopSpammingMaster.get() == false) {
                        masterClusterService.submitStateUpdateTask("spam", this);
                    }
                }
            });

        // even with the master under such pressure, all shards of the index can be assigned; in particular, after the primaries have
        // started there's a follow-up reroute at a higher priority than the spam
        createIndex("test");
        assertFalse(client().admin().cluster().prepareHealth().setWaitForGreenStatus().get().isTimedOut());

        stopSpammingMaster.set(true);
        assertFalse(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get().isTimedOut());

        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
            .putNull(ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING.getKey())));
    }

    public void testFollowupRerouteRejectsInvalidPriorities() {
        final String invalidPriority = randomFrom("IMMEDIATE", "LOW", "LANGUID");
        final ActionFuture<ClusterUpdateSettingsResponse> responseFuture = client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING.getKey(), invalidPriority))
            .execute();
        assertThat(expectThrows(IllegalArgumentException.class, responseFuture::actionGet).getMessage(),
            allOf(containsString(invalidPriority), containsString(ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING.getKey())));
    }

    private String randomPriority() {
        return randomFrom("normal", "high", "urgent", "NORMAL", "HIGH", "URGENT");
        // not "languid" (because we use that to wait for no pending tasks) nor "low" or "immediate" (because these are unreasonable)
    }

}
