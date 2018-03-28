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

package org.elasticsearch.indices.settings;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class UpdateSettingsIT extends ESIntegTestCase {
    public void testInvalidUpdateOnClosedIndex() {
        createIndex("test");
        assertAcked(client().admin().indices().prepareClose("test").get());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.analysis.char_filter.invalid_char.type", "invalid"))
                .get());
        assertEquals(exception.getMessage(), "Unknown char_filter type [invalid] for [invalid_char]");
    }

    public void testInvalidDynamicUpdate() {
        createIndex("test");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.dummy", "boom"))
                .execute()
                .actionGet());
        assertEquals(exception.getCause().getMessage(), "this setting goes boom");
        IndexMetaData indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertNotEquals(indexMetaData.getSettings().get("index.dummy"), "invalid dynamic value");
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DummySettingPlugin.class, FinalSettingPlugin.class);
    }

    public static class DummySettingPlugin extends Plugin {
        public static final Setting<String> DUMMY_SETTING = Setting.simpleString("index.dummy",
            Setting.Property.IndexScope, Setting.Property.Dynamic);

        public static final Setting.AffixSetting<String> DUMMY_ACCOUNT_USER = Setting.affixKeySetting("index.acc.", "user",
            k -> Setting.simpleString(k, Setting.Property.IndexScope, Setting.Property.Dynamic));
        public static final Setting<String> DUMMY_ACCOUNT_PW = Setting.affixKeySetting("index.acc.", "pw",
            k -> Setting.simpleString(k, Setting.Property.IndexScope, Setting.Property.Dynamic), DUMMY_ACCOUNT_USER);

        public static final Setting.AffixSetting<String> DUMMY_ACCOUNT_USER_CLUSTER = Setting.affixKeySetting("cluster.acc.", "user",
            k -> Setting.simpleString(k, Setting.Property.NodeScope, Setting.Property.Dynamic));
        public static final Setting<String> DUMMY_ACCOUNT_PW_CLUSTER = Setting.affixKeySetting("cluster.acc.", "pw",
            k -> Setting.simpleString(k, Setting.Property.NodeScope, Setting.Property.Dynamic), DUMMY_ACCOUNT_USER_CLUSTER);

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSettingsUpdateConsumer(DUMMY_SETTING, (s) -> {}, (s) -> {
                if (s.equals("boom"))
                    throw new IllegalArgumentException("this setting goes boom");
            });
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(DUMMY_SETTING, DUMMY_ACCOUNT_PW, DUMMY_ACCOUNT_USER,
                DUMMY_ACCOUNT_PW_CLUSTER, DUMMY_ACCOUNT_USER_CLUSTER);
        }
    }

    public static class FinalSettingPlugin extends Plugin {
        public static final Setting<String> FINAL_SETTING = Setting.simpleString("index.final",
            Setting.Property.IndexScope, Setting.Property.Final);
        @Override
        public void onIndexModule(IndexModule indexModule) {
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(FINAL_SETTING);
        }
    }

    public void testUpdateDependentClusterSettings() {
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
                .put("cluster.acc.test.pw", "asdf")).get());
        assertEquals("Missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

        iae = expectThrows(IllegalArgumentException.class, () ->
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put("cluster.acc.test.pw", "asdf")).get());
        assertEquals("Missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

        iae = expectThrows(IllegalArgumentException.class, () ->
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put("cluster.acc.test.pw", "asdf")).setPersistentSettings(Settings.builder()
            .put("cluster.acc.test.user", "asdf")).get());
        assertEquals("Missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

        if (randomBoolean()) {
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put("cluster.acc.test.pw", "asdf")
                .put("cluster.acc.test.user", "asdf")).get();
            iae = expectThrows(IllegalArgumentException.class, () ->
                client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                    .putNull("cluster.acc.test.user")).get());
            assertEquals("Missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .putNull("cluster.acc.test.pw")
                .putNull("cluster.acc.test.user")).get();
        } else {
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
                .put("cluster.acc.test.pw", "asdf")
                .put("cluster.acc.test.user", "asdf")).get();

            iae = expectThrows(IllegalArgumentException.class, () ->
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
                    .putNull("cluster.acc.test.user")).get());
            assertEquals("Missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
                .putNull("cluster.acc.test.pw")
                .putNull("cluster.acc.test.user")).get();
        }

    }

    public void testUpdateDependentIndexSettings() {
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
            prepareCreate("test",  Settings.builder().put("index.acc.test.pw", "asdf")).get());
        assertEquals("Missing required setting [index.acc.test.user] for setting [index.acc.test.pw]", iae.getMessage());

        createIndex("test");
        for (int i = 0; i < 2; i++) {
            if (i == 1) {
                // now do it on a closed index
                client().admin().indices().prepareClose("test").get();
            }

            iae = expectThrows(IllegalArgumentException.class, () ->
                client()
                    .admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(
                        Settings.builder()
                            .put("index.acc.test.pw", "asdf"))
                    .execute()
                    .actionGet());
            assertEquals("Missing required setting [index.acc.test.user] for setting [index.acc.test.pw]", iae.getMessage());

            // user has no dependency
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .put("index.acc.test.user", "asdf"))
                .execute()
                .actionGet();

            // now we are consistent
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .put("index.acc.test.pw", "test"))
                .execute()
                .actionGet();

            // now try to remove it and make sure it fails
            iae = expectThrows(IllegalArgumentException.class, () ->
                client()
                    .admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(
                        Settings.builder()
                            .putNull("index.acc.test.user"))
                    .execute()
                    .actionGet());
            assertEquals("Missing required setting [index.acc.test.user] for setting [index.acc.test.pw]", iae.getMessage());

            // now we are consistent
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .putNull("index.acc.test.pw")
                        .putNull("index.acc.test.user"))
                .execute()
                .actionGet();
        }
    }
    public void testResetDefaultWithWildcard() {
        createIndex("test");

        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put("index.refresh_interval", -1))
            .execute()
            .actionGet();
        IndexMetaData indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertEquals(indexMetaData.getSettings().get("index.refresh_interval"), "-1");
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), -1);
            }
        }
        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().putNull("index.ref*"))
            .execute()
            .actionGet();
        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertNull(indexMetaData.getSettings().get("index.refresh_interval"));
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), 1000);
            }
        }
    }

    public void testResetDefault() {
        createIndex("test");
        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(
                    Settings.builder()
                            .put("index.refresh_interval", -1)
                            .put("index.translog.flush_threshold_size", "1024b")
                            .put("index.translog.generation_threshold_size", "4096b"))
            .execute()
            .actionGet();
        IndexMetaData indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertEquals(indexMetaData.getSettings().get("index.refresh_interval"), "-1");
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), -1);
                assertEquals(indexService.getIndexSettings().getFlushThresholdSize().getBytes(), 1024);
                assertEquals(indexService.getIndexSettings().getGenerationThresholdSize().getBytes(), 4096);
            }
        }
        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().putNull("index.refresh_interval"))
            .execute()
            .actionGet();
        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertNull(indexMetaData.getSettings().get("index.refresh_interval"));
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), 1000);
                assertEquals(indexService.getIndexSettings().getFlushThresholdSize().getBytes(), 1024);
                assertEquals(indexService.getIndexSettings().getGenerationThresholdSize().getBytes(), 4096);
            }
        }
    }
    public void testOpenCloseUpdateSettings() throws Exception {
        createIndex("test");
        expectThrows(IllegalArgumentException.class, () ->
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder()
                    .put("index.refresh_interval", -1) // this one can change
                    .put("index.fielddata.cache", "none")) // this one can't
                .execute()
                .actionGet()
        );
        expectThrows(IllegalArgumentException.class, () ->
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder()
                    .put("index.refresh_interval", -1) // this one can change
                    .put("index.final", "no")) // this one can't
                .execute()
                .actionGet()
        );
        IndexMetaData indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.getSettings().get("index.refresh_interval"), nullValue());
        assertThat(indexMetaData.getSettings().get("index.fielddata.cache"), nullValue());
        assertThat(indexMetaData.getSettings().get("index.final"), nullValue());

        // Now verify via dedicated get settings api:
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), nullValue());
        assertThat(getSettingsResponse.getSetting("test", "index.fielddata.cache"), nullValue());
        assertThat(getSettingsResponse.getSetting("test", "index.final"), nullValue());

        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.refresh_interval", -1)) // this one can change
            .execute()
            .actionGet();

        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.getSettings().get("index.refresh_interval"), equalTo("-1"));
        // Now verify via dedicated get settings api:
        getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), equalTo("-1"));

        // now close the index, change the non dynamic setting, and see that it applies

        // Wait for the index to turn green before attempting to close it
        ClusterHealthResponse health =
            client()
                .admin()
                .cluster()
                .prepareHealth()
                .setTimeout("30s")
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForGreenStatus()
                .execute()
                .actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        client().admin().indices().prepareClose("test").execute().actionGet();

        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1))
            .execute()
            .actionGet();

        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(1));

        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder()
                .put("index.refresh_interval", "1s") // this one can change
                .put("index.fielddata.cache", "none")) // this one can't
            .execute()
            .actionGet();

        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.getSettings().get("index.refresh_interval"), equalTo("1s"));
        assertThat(indexMetaData.getSettings().get("index.fielddata.cache"), equalTo("none"));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder()
                    .put("index.refresh_interval", -1) // this one can change
                    .put("index.final", "no")) // this one really can't
                .execute()
                .actionGet()
        );
        assertThat(ex.getMessage(), containsString("final test setting [index.final], not updateable"));
        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.getSettings().get("index.refresh_interval"), equalTo("1s"));
        assertThat(indexMetaData.getSettings().get("index.final"), nullValue());


        // Now verify via dedicated get settings api:
        getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), equalTo("1s"));
        assertThat(getSettingsResponse.getSetting("test", "index.final"), nullValue());
    }

    public void testEngineGCDeletesSetting() throws InterruptedException {
        createIndex("test");
        client().prepareIndex("test", "type", "1").setSource("f", 1).get(); // set version to 1
        client().prepareDelete("test", "type", "1").get(); // sets version to 2
        // delete is still in cache this should work & set version to 3
        client().prepareIndex("test", "type", "1").setSource("f", 2).setVersion(2).get();
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.gc_deletes", 0)).get();

        client().prepareDelete("test", "type", "1").get(); // sets version to 4
        Thread.sleep(300); // wait for cache time to change TODO: this needs to be solved better. To be discussed.
        // delete is should not be in cache
        assertThrows(client().prepareIndex("test", "type", "1").setSource("f", 3).setVersion(4), VersionConflictEngineException.class);

    }

    public void testUpdateSettingsWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        Settings.Builder builder = Settings.builder().put("index.refresh_interval", -1);

        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(builder));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Closing an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareUpdateSettings("test").setSettings(builder));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }

}
