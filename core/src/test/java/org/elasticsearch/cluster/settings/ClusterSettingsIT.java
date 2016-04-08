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

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = TEST)
public class ClusterSettingsIT extends ESIntegTestCase {
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
            assertEquals(ex.getMessage(), "transient setting [no_idea_what_you_are_talking_about], not dynamically updateable");
        }
    }

    public void testDeleteIsAppliedFirst() {
        DiscoverySettings discoverySettings = getDiscoverySettings();

        assertEquals(discoverySettings.getPublishTimeout(), DiscoverySettings.PUBLISH_TIMEOUT_SETTING.get(Settings.EMPTY));
        assertTrue(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.get(Settings.EMPTY));

        ClusterUpdateSettingsResponse response = client().admin().cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.getKey(), false)
                .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s").build())
            .get();

        assertAcked(response);
        assertEquals(response.getTransientSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()), "1s");
        assertTrue(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.get(Settings.EMPTY));
        assertFalse(response.getTransientSettings().getAsBoolean(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.getKey(), null));

        response = client().admin().cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull((randomBoolean() ? "discovery.zen.*" : "*")).put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "2s"))
            .get();
        assertEquals(response.getTransientSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()), "2s");
        assertNull(response.getTransientSettings().getAsBoolean(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.getKey(), null));
    }

    public void testResetClusterSetting() {
        DiscoverySettings discoverySettings = getDiscoverySettings();

        assertThat(discoverySettings.getPublishTimeout(), equalTo(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.get(Settings.EMPTY)));
        assertThat(discoverySettings.getPublishDiff(), equalTo(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.get(Settings.EMPTY)));

        ClusterUpdateSettingsResponse response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s").build())
                .get();

        assertAcked(response);
        assertThat(response.getTransientSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()), equalTo("1s"));
        assertThat(discoverySettings.getPublishTimeout().seconds(), equalTo(1L));
        assertThat(discoverySettings.getPublishDiff(), equalTo(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.get(Settings.EMPTY)));


        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()))
                .get();

        assertAcked(response);
        assertNull(response.getTransientSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()));
        assertThat(discoverySettings.getPublishTimeout(), equalTo(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.get(Settings.EMPTY)));
        assertThat(discoverySettings.getPublishDiff(), equalTo(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.get(Settings.EMPTY)));

        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                        .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                        .put(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.getKey(), false).build())
                .get();

        assertAcked(response);
        assertThat(response.getTransientSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()), equalTo("1s"));
        assertThat(discoverySettings.getPublishTimeout().seconds(), equalTo(1L));
        assertFalse(discoverySettings.getPublishDiff());
        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull((randomBoolean() ? "discovery.zen.*" : "*")))
                .get();

        assertNull(response.getTransientSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()));
        assertNull(response.getTransientSettings().getAsMap().get(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.getKey()));
        assertThat(discoverySettings.getPublishTimeout(), equalTo(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.get(Settings.EMPTY)));
        assertThat(discoverySettings.getPublishDiff(), equalTo(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.get(Settings.EMPTY)));

        // now persistent
        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s").build())
                .get();

        assertAcked(response);
        assertThat(response.getPersistentSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()), equalTo("1s"));
        assertThat(discoverySettings.getPublishTimeout().seconds(), equalTo(1L));
        assertThat(discoverySettings.getPublishDiff(), equalTo(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.get(Settings.EMPTY)));


        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull((DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey())))
                .get();

        assertAcked(response);
        assertNull(response.getPersistentSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()));
        assertThat(discoverySettings.getPublishTimeout(), equalTo(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.get(Settings.EMPTY)));
        assertThat(discoverySettings.getPublishDiff(), equalTo(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.get(Settings.EMPTY)));


        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder()
                        .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                        .put(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.getKey(), false).build())
                .get();

        assertAcked(response);
        assertThat(response.getPersistentSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()), equalTo("1s"));
        assertThat(discoverySettings.getPublishTimeout().seconds(), equalTo(1L));
        assertFalse(discoverySettings.getPublishDiff());
        response = client().admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull((randomBoolean() ? "discovery.zen.*" : "*")))
                .get();

        assertNull(response.getPersistentSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()));
        assertNull(response.getPersistentSettings().getAsMap().get(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.getKey()));
        assertThat(discoverySettings.getPublishTimeout(), equalTo(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.get(Settings.EMPTY)));
        assertThat(discoverySettings.getPublishDiff(), equalTo(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.get(Settings.EMPTY)));
    }

    public void testClusterSettingsUpdateResponse() {
        String key1 = IndexStoreConfig.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING.getKey();
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
            .setTransientSettings(Settings.builder().putArray("transport.tracer.include", "internal:index/shard/recovery/*",
                "internal:gateway/local*"))
            .get();
        assertArrayEquals(clusterUpdateSettingsResponse.getTransientSettings().getAsArray("transport.tracer.include"), new String[] {"internal:index/shard/recovery/*",
            "internal:gateway/local*"});
    }

    public void testUpdateDiscoveryPublishTimeout() {

        DiscoverySettings discoverySettings = getDiscoverySettings();

        assertThat(discoverySettings.getPublishTimeout(), equalTo(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.get(Settings.EMPTY)));

        ClusterUpdateSettingsResponse response = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s").build())
                .get();

        assertAcked(response);
        assertThat(response.getTransientSettings().getAsMap().get(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey()), equalTo("1s"));
        assertThat(discoverySettings.getPublishTimeout().seconds(), equalTo(1L));

        try {
            client().admin().cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "whatever").build())
                    .get();
            fail("bogus value");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "Failed to parse setting [discovery.zen.publish_timeout] with value [whatever] as a time value: unit is missing or unrecognized");
        }

        assertThat(discoverySettings.getPublishTimeout().seconds(), equalTo(1L));

        try {
            client().admin().cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), -1).build())
                    .get();
            fail("bogus value");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "Failed to parse value [-1] for setting [discovery.zen.publish_timeout] must be >= 0s");
        }

        assertThat(discoverySettings.getPublishTimeout().seconds(), equalTo(1L));
    }

    private DiscoverySettings getDiscoverySettings() {return internalCluster().getInstance(Discovery.class).getDiscoverySettings();}

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
            Settings settings = Settings.builder().put(MetaData.SETTING_READ_ONLY_SETTING.getKey(), false).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());

        } finally {
            setClusterReadOnly(false);
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
            client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.refresh_interval", "10")).execute().actionGet();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[index.refresh_interval] with value [10]"));
            assertThat(e.getMessage(), containsString("unit is missing or unrecognized"));
        }
    }

    public void testLoggerLevelUpdate() {
        assertAcked(prepareCreate("test"));
        final String rootLevel = ESLoggerFactory.getRootLogger().getLevel();
        final String testLevel = ESLoggerFactory.getLogger("test").getLevel();
        try {
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put("logger._root", "BOOM")).execute().actionGet();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("No enum constant org.elasticsearch.common.logging.ESLoggerFactory.LogLevel.BOOM", e.getMessage());
        }

        try {
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put("logger.test", "TRACE").put("logger._root", "trace")).execute().actionGet();
            assertEquals("TRACE", ESLoggerFactory.getLogger("test").getLevel());
            assertEquals("TRACE", ESLoggerFactory.getRootLogger().getLevel());
        } finally {
            if (randomBoolean()) {
                client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().putNull("logger.test").putNull("logger._root")).execute().actionGet();
            } else {
                client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().putNull("logger.*")).execute().actionGet();
            }
            assertEquals(testLevel, ESLoggerFactory.getLogger("test").getLevel());
            assertEquals(rootLevel, ESLoggerFactory.getRootLogger().getLevel());
        }
    }

}
