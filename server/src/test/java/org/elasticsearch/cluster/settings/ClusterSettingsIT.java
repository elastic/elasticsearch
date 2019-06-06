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

package org.elasticsearch.cluster.settings;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.Arrays;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ClusterSettingsIT extends ESIntegTestCase {

    @After
    public void cleanup() throws Exception {
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().putNull("*"))
            .setTransientSettings(Settings.builder().putNull("*")));
    }

    public void testClusterNonExistingSettingsUpdate() {
        String key1 = "no_idea_what_you_are_talking_about";
        int value1 = 10;
        try {
            client().admin().cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(key1, value1).build())
                    .get();
            fail("bogus value");
        } catch (IllegalArgumentException ex) {
            assertEquals("transient setting [no_idea_what_you_are_talking_about], not recognized", ex.getMessage());
        }
    }

    public void testDeleteIsAppliedFirst() {
        final Setting<Integer> INITIAL_RECOVERIES = CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
        final Setting<TimeValue> REROUTE_INTERVAL = CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING;

        ClusterUpdateSettingsResponse response = client().admin().cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(INITIAL_RECOVERIES.getKey(), 7)
                .put(REROUTE_INTERVAL.getKey(), "42s").build())
            .get();

        assertAcked(response);
        assertThat(INITIAL_RECOVERIES.get(response.getTransientSettings()), equalTo(7));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(7));
        assertThat(REROUTE_INTERVAL.get(response.getTransientSettings()), equalTo(TimeValue.timeValueSeconds(42)));
        assertThat(clusterService().getClusterSettings().get(REROUTE_INTERVAL), equalTo(TimeValue.timeValueSeconds(42)));

        response = client().admin().cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull((randomBoolean() ? "cluster.routing.*" : "*"))
                .put(REROUTE_INTERVAL.getKey(), "43s"))
            .get();
        assertThat(INITIAL_RECOVERIES.get(response.getTransientSettings()), equalTo(INITIAL_RECOVERIES.get(Settings.EMPTY)));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(INITIAL_RECOVERIES.get(Settings.EMPTY)));
        assertThat(REROUTE_INTERVAL.get(response.getTransientSettings()), equalTo(TimeValue.timeValueSeconds(43)));
        assertThat(clusterService().getClusterSettings().get(REROUTE_INTERVAL), equalTo(TimeValue.timeValueSeconds(43)));
    }

    public void testResetClusterSetting() {
        final Setting<Integer> INITIAL_RECOVERIES = CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
        final Setting<TimeValue> REROUTE_INTERVAL = CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING;

        ClusterUpdateSettingsResponse response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(INITIAL_RECOVERIES.getKey(), 7).build())
                .get();

        assertAcked(response);
        assertThat(INITIAL_RECOVERIES.get(response.getTransientSettings()), equalTo(7));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(7));

        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(INITIAL_RECOVERIES.getKey()))
                .get();

        assertAcked(response);
        assertNull(response.getTransientSettings().get(INITIAL_RECOVERIES.getKey()));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES),
            equalTo(INITIAL_RECOVERIES.get(Settings.EMPTY)));

        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                        .put(INITIAL_RECOVERIES.getKey(), 8)
                        .put(REROUTE_INTERVAL.getKey(), "43s").build())
                .get();

        assertAcked(response);
        assertThat(INITIAL_RECOVERIES.get(response.getTransientSettings()), equalTo(8));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(8));
        assertThat(REROUTE_INTERVAL.get(response.getTransientSettings()), equalTo(TimeValue.timeValueSeconds(43)));
        assertThat(clusterService().getClusterSettings().get(REROUTE_INTERVAL), equalTo(TimeValue.timeValueSeconds(43)));
        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull((randomBoolean() ? "cluster.routing.*" : "*")))
                .get();

        assertThat(INITIAL_RECOVERIES.get(response.getTransientSettings()), equalTo(INITIAL_RECOVERIES.get(Settings.EMPTY)));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(INITIAL_RECOVERIES.get(Settings.EMPTY)));
        assertThat(REROUTE_INTERVAL.get(response.getTransientSettings()), equalTo(REROUTE_INTERVAL.get(Settings.EMPTY)));
        assertThat(clusterService().getClusterSettings().get(REROUTE_INTERVAL), equalTo(REROUTE_INTERVAL.get(Settings.EMPTY)));

        // now persistent
        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(INITIAL_RECOVERIES.getKey(), 9).build())
                .get();

        assertAcked(response);
        assertThat(INITIAL_RECOVERIES.get(response.getPersistentSettings()), equalTo(9));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(9));

        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(INITIAL_RECOVERIES.getKey()))
                .get();

        assertAcked(response);
        assertThat(INITIAL_RECOVERIES.get(response.getPersistentSettings()), equalTo(INITIAL_RECOVERIES.get(Settings.EMPTY)));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(INITIAL_RECOVERIES.get(Settings.EMPTY)));

        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder()
                        .put(INITIAL_RECOVERIES.getKey(), 10)
                        .put(REROUTE_INTERVAL.getKey(), "44s").build())
                .get();

        assertAcked(response);
        assertThat(INITIAL_RECOVERIES.get(response.getPersistentSettings()), equalTo(10));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(10));
        assertThat(REROUTE_INTERVAL.get(response.getPersistentSettings()), equalTo(TimeValue.timeValueSeconds(44)));
        assertThat(clusterService().getClusterSettings().get(REROUTE_INTERVAL), equalTo(TimeValue.timeValueSeconds(44)));
        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull((randomBoolean() ? "cluster.routing.*" : "*")))
                .get();

        assertThat(INITIAL_RECOVERIES.get(response.getPersistentSettings()), equalTo(INITIAL_RECOVERIES.get(Settings.EMPTY)));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(INITIAL_RECOVERIES.get(Settings.EMPTY)));
        assertThat(REROUTE_INTERVAL.get(response.getPersistentSettings()), equalTo(REROUTE_INTERVAL.get(Settings.EMPTY)));
        assertThat(clusterService().getClusterSettings().get(REROUTE_INTERVAL), equalTo(REROUTE_INTERVAL.get(Settings.EMPTY)));
    }

    public void testClusterSettingsUpdateResponse() {
        String key1 = RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey();
        int value1 = 10;

        String key2 = EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey();
        String value2 =  EnableAllocationDecider.Allocation.NONE.name();

        Settings transientSettings1 = Settings.builder().put(key1, value1, ByteSizeUnit.BYTES).build();
        Settings persistentSettings1 = Settings.builder().put(key2, value2).build();

        ClusterUpdateSettingsResponse response1 = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(transientSettings1)
                .setPersistentSettings(persistentSettings1)
                .execute()
                .actionGet();

        assertAcked(response1);
        assertThat(response1.getTransientSettings().get(key1), notNullValue());
        assertThat(response1.getTransientSettings().get(key2), nullValue());
        assertThat(response1.getPersistentSettings().get(key1), nullValue());
        assertThat(response1.getPersistentSettings().get(key2), notNullValue());

        Settings transientSettings2 = Settings.builder().put(key1, value1, ByteSizeUnit.BYTES).put(key2, value2).build();
        Settings persistentSettings2 = Settings.EMPTY;

        ClusterUpdateSettingsResponse response2 = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(transientSettings2)
                .setPersistentSettings(persistentSettings2)
                .execute()
                .actionGet();

        assertAcked(response2);
        assertThat(response2.getTransientSettings().get(key1), notNullValue());
        assertThat(response2.getTransientSettings().get(key2), notNullValue());
        assertThat(response2.getPersistentSettings().get(key1), nullValue());
        assertThat(response2.getPersistentSettings().get(key2), nullValue());

        Settings transientSettings3 = Settings.EMPTY;
        Settings persistentSettings3 = Settings.builder().put(key1, value1, ByteSizeUnit.BYTES).put(key2, value2).build();

        ClusterUpdateSettingsResponse response3 = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(transientSettings3)
                .setPersistentSettings(persistentSettings3)
                .execute()
                .actionGet();

        assertAcked(response3);
        assertThat(response3.getTransientSettings().get(key1), nullValue());
        assertThat(response3.getTransientSettings().get(key2), nullValue());
        assertThat(response3.getPersistentSettings().get(key1), notNullValue());
        assertThat(response3.getPersistentSettings().get(key2), notNullValue());
    }

    public void testCanUpdateTracerSettings() {
        ClusterUpdateSettingsResponse clusterUpdateSettingsResponse = client().admin().cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putList("transport.tracer.include", "internal:index/shard/recovery/*",
                "internal:gateway/local*"))
            .get();
        assertEquals(clusterUpdateSettingsResponse.getTransientSettings().getAsList("transport.tracer.include"),
            Arrays.asList("internal:index/shard/recovery/*", "internal:gateway/local*"));
    }

    public void testUpdateSettings() {
        final Setting<Integer> INITIAL_RECOVERIES = CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;

        ClusterUpdateSettingsResponse response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(INITIAL_RECOVERIES.getKey(), 42).build())
                .get();

        assertAcked(response);
        assertThat(INITIAL_RECOVERIES.get(response.getTransientSettings()), equalTo(42));
        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(42));

        try {
            client().admin().cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(INITIAL_RECOVERIES.getKey(), "whatever").build())
                    .get();
            fail("bogus value");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "Failed to parse value [whatever] for setting [" + INITIAL_RECOVERIES.getKey() + "]");
        }

        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(42));

        try {
            client().admin().cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder()
                        .put(INITIAL_RECOVERIES.getKey(), -1).build())
                    .get();
            fail("bogus value");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "Failed to parse value [-1] for setting [" + INITIAL_RECOVERIES.getKey() + "] must be >= 0");
        }

        assertThat(clusterService().getClusterSettings().get(INITIAL_RECOVERIES), equalTo(42));
    }

    public void testClusterUpdateSettingsWithBlocks() {
        String key1 = "cluster.routing.allocation.enable";
        Settings transientSettings = Settings.builder().put(key1, EnableAllocationDecider.Allocation.NONE.name()).build();

        String key2 = "cluster.routing.allocation.node_concurrent_recoveries";
        Settings persistentSettings = Settings.builder().put(key2, "5").build();

        ClusterUpdateSettingsRequestBuilder request = client().admin().cluster().prepareUpdateSettings()
                                                                                .setTransientSettings(transientSettings)
                                                                                .setPersistentSettings(persistentSettings);

        // Cluster settings updates are blocked when the cluster is read only
        try {
            setClusterReadOnly(true);
            assertBlocked(request, MetaData.CLUSTER_READ_ONLY_BLOCK);

            // But it's possible to update the settings to update the "cluster.blocks.read_only" setting
            Settings settings = Settings.builder().putNull(MetaData.SETTING_READ_ONLY_SETTING.getKey()).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());

        } finally {
            setClusterReadOnly(false);
        }

        // Cluster settings updates are blocked when the cluster is read only
        try {
            // But it's possible to update the settings to update the "cluster.blocks.read_only" setting
            Settings settings = Settings.builder().put(MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
            assertBlocked(request, MetaData.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
        } finally {
            // But it's possible to update the settings to update the "cluster.blocks.read_only" setting
            Settings s = Settings.builder().putNull(MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey()).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(s).get());
        }

        // It should work now
        ClusterUpdateSettingsResponse response = request.execute().actionGet();

        assertAcked(response);
        assertThat(response.getTransientSettings().get(key1), notNullValue());
        assertThat(response.getTransientSettings().get(key2), nullValue());
        assertThat(response.getPersistentSettings().get(key1), nullValue());
        assertThat(response.getPersistentSettings().get(key2), notNullValue());
    }

    public void testMissingUnits() {
        assertAcked(prepareCreate("test"));

        try {
            client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.refresh_interval", "10")).execute().actionGet();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[index.refresh_interval] with value [10]"));
            assertThat(e.getMessage(), containsString("unit is missing or unrecognized"));
        }
    }

    public void testLoggerLevelUpdate() {
        assertAcked(prepareCreate("test"));

        final Level level = LogManager.getRootLogger().getLevel();

        final IllegalArgumentException e =
            expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().cluster().prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put("logger._root", "BOOM")).execute().actionGet());
        assertEquals("Unknown level constant [BOOM].", e.getMessage());

        try {
            final Settings.Builder testSettings = Settings.builder().put("logger.test", "TRACE").put("logger._root", "trace");
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(testSettings).execute().actionGet();
            assertEquals(Level.TRACE, LogManager.getLogger("test").getLevel());
            assertEquals(Level.TRACE, LogManager.getRootLogger().getLevel());
        } finally {
            if (randomBoolean()) {
                final Settings.Builder defaultSettings = Settings.builder().putNull("logger.test").putNull("logger._root");
                client().admin().cluster().prepareUpdateSettings().setTransientSettings(defaultSettings).execute().actionGet();
            } else {
                final Settings.Builder defaultSettings = Settings.builder().putNull("logger.*");
                client().admin().cluster().prepareUpdateSettings().setTransientSettings(defaultSettings).execute().actionGet();
            }
            assertEquals(level, LogManager.getLogger("test").getLevel());
            assertEquals(level, LogManager.getRootLogger().getLevel());
        }
    }

    public void testUserMetadata() {
        String key = "cluster.metadata." + randomAlphaOfLengthBetween(5, 20);
        String value = randomRealisticUnicodeOfCodepointLengthBetween(5, 50);
        String updatedValue = randomRealisticUnicodeOfCodepointLengthBetween(5, 50);
        logger.info("Attempting to store [{}]: [{}], then update to [{}]", key, value, updatedValue);

        final Settings settings = Settings.builder().put(key, value).build();
        final Settings updatedSettings = Settings.builder().put(key, updatedValue).build();
        if (randomBoolean()) {
            logger.info("Using persistent settings");

            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).execute().actionGet();
            ClusterStateResponse state = client().admin().cluster().prepareState().execute().actionGet();
            assertEquals(value, state.getState().getMetaData().persistentSettings().get(key));

            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(updatedSettings).execute().actionGet();
            ClusterStateResponse updatedState = client().admin().cluster().prepareState().execute().actionGet();
            assertEquals(updatedValue, updatedState.getState().getMetaData().persistentSettings().get(key));
        } else {
            logger.info("Using transient settings");
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();
            ClusterStateResponse state = client().admin().cluster().prepareState().execute().actionGet();
            assertEquals(value, state.getState().getMetaData().transientSettings().get(key));

            client().admin().cluster().prepareUpdateSettings().setTransientSettings(updatedSettings).execute().actionGet();
            ClusterStateResponse updatedState = client().admin().cluster().prepareState().execute().actionGet();
            assertEquals(updatedValue, updatedState.getState().getMetaData().transientSettings().get(key));
        }
    }

}
