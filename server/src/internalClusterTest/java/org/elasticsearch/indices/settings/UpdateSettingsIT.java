/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.settings;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertRequestBuilderThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class UpdateSettingsIT extends ESIntegTestCase {
    public void testInvalidUpdateOnClosedIndex() {
        createIndex("test");
        assertAcked(indicesAdmin().prepareClose("test").get());
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.analysis.char_filter.invalid_char.type", "invalid"))
                .get()
        );
        assertEquals(exception.getMessage(), "Unknown char_filter type [invalid] for [invalid_char]");
    }

    public void testInvalidDynamicUpdate() {
        createIndex("test");
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.dummy", "boom"))
                .execute()
                .actionGet()
        );
        assertEquals(exception.getCause().getMessage(), "this setting goes boom");
        IndexMetadata indexMetadata = clusterAdmin().prepareState().execute().actionGet().getState().metadata().index("test");
        assertNotEquals(indexMetadata.getSettings().get("index.dummy"), "invalid dynamic value");
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DummySettingPlugin.class, FinalSettingPlugin.class);
    }

    public static class DummySettingPlugin extends Plugin {
        public static final Setting<String> DUMMY_SETTING = Setting.simpleString(
            "index.dummy",
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        );

        public static final Setting.AffixSetting<String> DUMMY_ACCOUNT_USER = Setting.affixKeySetting(
            "index.acc.",
            "user",
            k -> Setting.simpleString(k, Setting.Property.IndexScope, Setting.Property.Dynamic)
        );
        public static final Setting<String> DUMMY_ACCOUNT_PW = Setting.affixKeySetting(
            "index.acc.",
            "pw",
            k -> Setting.simpleString(k, Setting.Property.IndexScope, Setting.Property.Dynamic),
            () -> DUMMY_ACCOUNT_USER
        );

        public static final Setting.AffixSetting<String> DUMMY_ACCOUNT_USER_CLUSTER = Setting.affixKeySetting(
            "cluster.acc.",
            "user",
            k -> Setting.simpleString(k, Setting.Property.NodeScope, Setting.Property.Dynamic)
        );
        public static final Setting<String> DUMMY_ACCOUNT_PW_CLUSTER = Setting.affixKeySetting(
            "cluster.acc.",
            "pw",
            k -> Setting.simpleString(k, Setting.Property.NodeScope, Setting.Property.Dynamic),
            () -> DUMMY_ACCOUNT_USER_CLUSTER
        );

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSettingsUpdateConsumer(DUMMY_SETTING, (s) -> {}, (s) -> {
                if (s.equals("boom")) throw new IllegalArgumentException("this setting goes boom");
            });
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(DUMMY_SETTING, DUMMY_ACCOUNT_PW, DUMMY_ACCOUNT_USER, DUMMY_ACCOUNT_PW_CLUSTER, DUMMY_ACCOUNT_USER_CLUSTER);
        }
    }

    public static class FinalSettingPlugin extends Plugin {
        public static final Setting<String> FINAL_SETTING = Setting.simpleString(
            "index.final",
            Setting.Property.IndexScope,
            Setting.Property.Final
        );

        @Override
        public void onIndexModule(IndexModule indexModule) {}

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(FINAL_SETTING);
        }
    }

    /**
     * Needed by {@link UpdateSettingsIT#testEngineGCDeletesSetting()}
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("thread_pool.estimated_time_interval", 0).build();
    }

    public void testUpdateDependentClusterSettings() {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> clusterAdmin().prepareUpdateSettings().setPersistentSettings(Settings.builder().put("cluster.acc.test.pw", "asdf")).get()
        );
        assertEquals("missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

        iae = expectThrows(
            IllegalArgumentException.class,
            () -> clusterAdmin().prepareUpdateSettings().setTransientSettings(Settings.builder().put("cluster.acc.test.pw", "asdf")).get()
        );
        assertEquals("missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

        iae = expectThrows(
            IllegalArgumentException.class,
            () -> clusterAdmin().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.acc.test.pw", "asdf"))
                .setPersistentSettings(Settings.builder().put("cluster.acc.test.user", "asdf"))
                .get()
        );
        assertEquals("missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

        if (randomBoolean()) {
            clusterAdmin().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.acc.test.pw", "asdf").put("cluster.acc.test.user", "asdf"))
                .get();
            iae = expectThrows(
                IllegalArgumentException.class,
                () -> clusterAdmin().prepareUpdateSettings().setTransientSettings(Settings.builder().putNull("cluster.acc.test.user")).get()
            );
            assertEquals("missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());
            clusterAdmin().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull("cluster.acc.test.pw").putNull("cluster.acc.test.user"))
                .get();
        } else {
            clusterAdmin().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("cluster.acc.test.pw", "asdf").put("cluster.acc.test.user", "asdf"))
                .get();

            iae = expectThrows(
                IllegalArgumentException.class,
                () -> clusterAdmin().prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().putNull("cluster.acc.test.user"))
                    .get()
            );
            assertEquals("missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

            updateClusterSettings(Settings.builder().putNull("cluster.acc.test.pw").putNull("cluster.acc.test.user"));
        }
    }

    public void testUpdateDependentIndexSettings() {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test", Settings.builder().put("index.acc.test.pw", "asdf")).get()
        );
        assertEquals("missing required setting [index.acc.test.user] for setting [index.acc.test.pw]", iae.getMessage());

        createIndex("test");
        for (int i = 0; i < 2; i++) {
            if (i == 1) {
                // now do it on a closed index
                indicesAdmin().prepareClose("test").get();
            }

            iae = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.acc.test.pw", "asdf"))
                    .execute()
                    .actionGet()
            );
            assertEquals("missing required setting [index.acc.test.user] for setting [index.acc.test.pw]", iae.getMessage());

            // user has no dependency
            indicesAdmin().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.acc.test.user", "asdf"))
                .execute()
                .actionGet();

            // now we are consistent
            indicesAdmin().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.acc.test.pw", "test"))
                .execute()
                .actionGet();

            // now try to remove it and make sure it fails
            iae = expectThrows(
                IllegalArgumentException.class,
                () -> indicesAdmin().prepareUpdateSettings("test")
                    .setSettings(Settings.builder().putNull("index.acc.test.user"))
                    .execute()
                    .actionGet()
            );
            assertEquals("missing required setting [index.acc.test.user] for setting [index.acc.test.pw]", iae.getMessage());

            // now we are consistent
            indicesAdmin().prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull("index.acc.test.pw").putNull("index.acc.test.user"))
                .execute()
                .actionGet();
        }
    }

    public void testResetDefaultWithWildcard() {
        createIndex("test");

        indicesAdmin().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.refresh_interval", -1))
            .execute()
            .actionGet();
        IndexMetadata indexMetadata = clusterAdmin().prepareState().execute().actionGet().getState().metadata().index("test");
        assertEquals(indexMetadata.getSettings().get("index.refresh_interval"), "-1");
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), -1);
            }
        }
        indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().putNull("index.ref*")).execute().actionGet();
        indexMetadata = clusterAdmin().prepareState().execute().actionGet().getState().metadata().index("test");
        assertNull(indexMetadata.getSettings().get("index.refresh_interval"));
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), 1000);
            }
        }
    }

    public void testResetDefault() {
        createIndex("test");
        indicesAdmin().prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put("index.refresh_interval", -1)
                    .put("index.translog.flush_threshold_size", "1024b")
                    .put("index.translog.generation_threshold_size", "4096b")
            )
            .execute()
            .actionGet();
        IndexMetadata indexMetadata = clusterAdmin().prepareState().execute().actionGet().getState().metadata().index("test");
        assertEquals(indexMetadata.getSettings().get("index.refresh_interval"), "-1");
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), -1);
                assertEquals(indexService.getIndexSettings().getFlushThresholdSize(new ByteSizeValue(1, ByteSizeUnit.TB)).getBytes(), 1024);
                assertEquals(indexService.getIndexSettings().getGenerationThresholdSize().getBytes(), 4096);
            }
        }
        indicesAdmin().prepareUpdateSettings("test")
            .setSettings(Settings.builder().putNull("index.refresh_interval"))
            .execute()
            .actionGet();
        indexMetadata = clusterAdmin().prepareState().execute().actionGet().getState().metadata().index("test");
        assertNull(indexMetadata.getSettings().get("index.refresh_interval"));
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), 1000);
                assertEquals(indexService.getIndexSettings().getFlushThresholdSize(new ByteSizeValue(1, ByteSizeUnit.TB)).getBytes(), 1024);
                assertEquals(indexService.getIndexSettings().getGenerationThresholdSize().getBytes(), 4096);
            }
        }
    }

    public void testOpenCloseUpdateSettings() throws Exception {
        createIndex("test");
        expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .put("index.refresh_interval", -1) // this one can change
                        .put("index.fielddata.cache", "none")
                ) // this one can't
                .execute()
                .actionGet()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .put("index.refresh_interval", -1) // this one can change
                        .put("index.final", "no")
                ) // this one can't
                .execute()
                .actionGet()
        );
        IndexMetadata indexMetadata = clusterAdmin().prepareState().execute().actionGet().getState().metadata().index("test");
        assertThat(indexMetadata.getSettings().get("index.refresh_interval"), nullValue());
        assertThat(indexMetadata.getSettings().get("index.fielddata.cache"), nullValue());
        assertThat(indexMetadata.getSettings().get("index.final"), nullValue());

        // Now verify via dedicated get settings api:
        GetSettingsResponse getSettingsResponse = indicesAdmin().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), nullValue());
        assertThat(getSettingsResponse.getSetting("test", "index.fielddata.cache"), nullValue());
        assertThat(getSettingsResponse.getSetting("test", "index.final"), nullValue());

        indicesAdmin().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.refresh_interval", -1)) // this one can change
            .execute()
            .actionGet();

        indexMetadata = clusterAdmin().prepareState().execute().actionGet().getState().metadata().index("test");
        assertThat(indexMetadata.getSettings().get("index.refresh_interval"), equalTo("-1"));
        // Now verify via dedicated get settings api:
        getSettingsResponse = indicesAdmin().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), equalTo("-1"));

        // now close the index, change the non dynamic setting, and see that it applies

        // Wait for the index to turn green before attempting to close it
        ClusterHealthResponse health = clusterAdmin().prepareHealth()
            .setTimeout("30s")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .execute()
            .actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        indicesAdmin().prepareClose("test").execute().actionGet();

        indicesAdmin().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .execute()
            .actionGet();

        indexMetadata = clusterAdmin().prepareState().execute().actionGet().getState().metadata().index("test");
        assertThat(indexMetadata.getNumberOfReplicas(), equalTo(1));

        indicesAdmin().prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put("index.refresh_interval", "1s") // this one can change
                    .put("index.fielddata.cache", "none")
            ) // this one can't
            .execute()
            .actionGet();

        indexMetadata = clusterAdmin().prepareState().execute().actionGet().getState().metadata().index("test");
        assertThat(indexMetadata.getSettings().get("index.refresh_interval"), equalTo("1s"));
        assertThat(indexMetadata.getSettings().get("index.fielddata.cache"), equalTo("none"));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .put("index.refresh_interval", -1) // this one can change
                        .put("index.final", "no")
                ) // this one really can't
                .execute()
                .actionGet()
        );
        assertThat(ex.getMessage(), containsString("final test setting [index.final], not updateable"));
        indexMetadata = clusterAdmin().prepareState().execute().actionGet().getState().metadata().index("test");
        assertThat(indexMetadata.getSettings().get("index.refresh_interval"), equalTo("1s"));
        assertThat(indexMetadata.getSettings().get("index.final"), nullValue());

        // Now verify via dedicated get settings api:
        getSettingsResponse = indicesAdmin().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), equalTo("1s"));
        assertThat(getSettingsResponse.getSetting("test", "index.final"), nullValue());
    }

    public void testEngineGCDeletesSetting() throws Exception {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource("f", 1).setVersionType(VersionType.EXTERNAL).setVersion(1).get();
        client().prepareDelete("test", "1").setVersionType(VersionType.EXTERNAL).setVersion(2).get();
        // delete is still in cache this should fail
        assertRequestBuilderThrows(
            client().prepareIndex("test").setId("1").setSource("f", 3).setVersionType(VersionType.EXTERNAL).setVersion(1),
            VersionConflictEngineException.class
        );

        assertAcked(indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.gc_deletes", 0)));

        client().prepareDelete("test", "1").setVersionType(VersionType.EXTERNAL).setVersion(4).get();

        // Make sure the time has advanced for InternalEngine#resolveDocVersion()
        for (ThreadPool threadPool : internalCluster().getInstances(ThreadPool.class)) {
            long startTime = threadPool.relativeTimeInMillis();
            assertBusy(() -> assertThat(threadPool.relativeTimeInMillis(), greaterThan(startTime)));
        }

        // delete should not be in cache
        client().prepareIndex("test").setId("1").setSource("f", 2).setVersionType(VersionType.EXTERNAL).setVersion(1);
    }

    public void testUpdateSettingsWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        Settings.Builder builder = Settings.builder().put("index.refresh_interval", -1);

        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertAcked(indicesAdmin().prepareUpdateSettings("test").setSettings(builder));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Closing an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(indicesAdmin().prepareUpdateSettings("test").setSettings(builder));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }

    public void testSettingsVersion() {
        createIndex("test");
        ensureGreen("test");

        {
            final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
            assertAcked(
                indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.refresh_interval", "500ms")).get()
            );
            final long newSettingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
            assertThat(newSettingsVersion, equalTo(1 + settingsVersion));
        }

        {
            final boolean block = randomBoolean();
            assertAcked(
                indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.blocks.read_only", block)).get()
            );
            final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
            assertAcked(
                indicesAdmin().prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.blocks.read_only", block == false))
                    .get()
            );
            final long newSettingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
            assertThat(newSettingsVersion, equalTo(1 + settingsVersion));

            // if the read-only block is present, remove it
            if (block == false) {
                assertAcked(
                    indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.blocks.read_only", false)).get()
                );
            }
        }
    }

    public void testSettingsVersionUnchanged() {
        createIndex("test");
        ensureGreen("test");

        {
            final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
            final String refreshInterval = indicesAdmin().prepareGetSettings("test").get().getSetting("test", "index.refresh_interval");
            assertAcked(
                indicesAdmin().prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.refresh_interval", refreshInterval))
                    .get()
            );
            final long newSettingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
            assertThat(newSettingsVersion, equalTo(settingsVersion));
        }

        {
            final boolean block = randomBoolean();
            assertAcked(
                indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.blocks.read_only", block)).get()
            );
            // now put the same block again
            final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
            assertAcked(
                indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.blocks.read_only", block)).get()
            );
            final long newSettingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
            assertThat(newSettingsVersion, equalTo(settingsVersion));

            // if the read-only block is present, remove it
            if (block) {
                assertAcked(
                    indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.blocks.read_only", false)).get()
                );
            }
        }
    }

    /**
     * The setting {@link IndexMetadata#SETTING_NUMBER_OF_REPLICAS} is special due to handling in
     * {@link IndexMetadata.Builder#numberOfReplicas(int)}. Therefore we have a dedicated test that this setting is handled properly with
     * respect to settings version when applying a settings change that does not change the number of replicas.
     */
    public void testNumberOfReplicasSettingsVersionUnchanged() {
        createIndex("test");

        final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
        final int numberOfReplicas = Integer.valueOf(
            indicesAdmin().prepareGetSettings("test").get().getSetting("test", "index.number_of_replicas")
        );
        assertAcked(
            indicesAdmin().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.number_of_replicas", numberOfReplicas))
                .get()
        );
        final long newSettingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
        assertThat(newSettingsVersion, equalTo(settingsVersion));
    }

    /**
     * The setting {@link IndexMetadata#SETTING_NUMBER_OF_REPLICAS} is special due to handling in
     * {@link IndexMetadata.Builder#numberOfReplicas(int)}. Therefore we have a dedicated test that this setting is handled properly with
     * respect to settings version when changing the number of replicas.
     */
    public void testNumberOfReplicasSettingsVersion() {
        createIndex("test");

        final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
        final int numberOfReplicas = Integer.valueOf(
            indicesAdmin().prepareGetSettings("test").get().getSetting("test", "index.number_of_replicas")
        );
        assertAcked(
            indicesAdmin().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.number_of_replicas", 1 + numberOfReplicas))
                .get()
        );
        final long newSettingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
        assertThat(newSettingsVersion, equalTo(1 + settingsVersion));
    }

    /*
     * Test that we are able to set the setting index.number_of_replicas to the default.
     */
    public void testDefaultNumberOfReplicasOnOpenIndices() {
        runTestDefaultNumberOfReplicasTest(false);
    }

    public void testDefaultNumberOfReplicasOnClosedIndices() {
        runTestDefaultNumberOfReplicasTest(true);
    }

    private void runTestDefaultNumberOfReplicasTest(final boolean closeIndex) {
        if (randomBoolean()) {
            assertAcked(
                indicesAdmin().prepareCreate("test")
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(1, 8)))
            );
        } else {
            assertAcked(indicesAdmin().prepareCreate("test"));
        }

        if (closeIndex) {
            assertAcked(indicesAdmin().prepareClose("test"));
        }

        /*
         * Previous versions of Elasticsearch would throw an exception that the number of replicas had to have a value, and could not be
         * null. In the update settings logic, we ensure this by providing an explicit default value if the setting is set to null.
         */
        assertAcked(
            indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().putNull(IndexMetadata.SETTING_NUMBER_OF_REPLICAS))
        );

        final GetSettingsResponse response = indicesAdmin().prepareGetSettings("test").get();

        // we removed the setting but it should still have an explicit value since index metadata requires this
        assertTrue(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(response.getIndexToSettings().get("test")));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(response.getIndexToSettings().get("test")), equalTo(1));
    }

    public void testNoopUpdate() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final ClusterService clusterService = internalCluster().getAnyMasterNodeInstance(ClusterService.class);
        assertAcked(indicesAdmin().prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)));

        ClusterState currentState = clusterService.state();
        assertAcked(
            indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        assertNotSame(currentState, clusterService.state());
        clusterAdmin().prepareHealth()
            .setWaitForGreenStatus()
            .setWaitForNoInitializingShards(true)
            .setWaitForNoRelocatingShards(true)
            .setWaitForEvents(Priority.LANGUID)
            .setTimeout(TimeValue.MAX_VALUE)
            .get();
        currentState = clusterService.state();

        assertAcked(
            indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        assertSame(clusterService.state(), currentState);

        assertAcked(
            indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().putNull(IndexMetadata.SETTING_NUMBER_OF_REPLICAS))
        );
        assertSame(clusterService.state(), currentState);

        assertAcked(
            indicesAdmin().prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull(SETTING_BLOCKS_READ).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        assertSame(currentState, clusterService.state());

        assertAcked(
            indicesAdmin().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(SETTING_BLOCKS_READ, true).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        assertNotSame(currentState, clusterService.state());
        currentState = clusterService.state();

        assertAcked(indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put(SETTING_BLOCKS_READ, true)));
        assertSame(currentState, clusterService.state());

        assertAcked(indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().putNull(SETTING_BLOCKS_READ)));
        assertNotSame(currentState, clusterService.state());
    }

    public void testAllSettingStringInterned() {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String index1 = "index-1";
        final String index2 = "index-2";
        createIndex(index1, index2);
        final ClusterService clusterServiceMaster = internalCluster().getInstance(ClusterService.class, masterNode);
        final ClusterService clusterServiceData = internalCluster().getInstance(ClusterService.class, dataNode);
        final Settings index1SettingsMaster = clusterServiceMaster.state().metadata().index(index1).getSettings();
        final Settings index1SettingsData = clusterServiceData.state().metadata().index(index1).getSettings();
        assertNotSame(index1SettingsMaster, index1SettingsData);
        assertSame(index1SettingsMaster.get(IndexMetadata.SETTING_INDEX_UUID), index1SettingsData.get(IndexMetadata.SETTING_INDEX_UUID));

        // Create a list of not interned strings to make sure interning setting values works
        final List<String> queryFieldsSetting = List.of(new String("foo"), new String("bar"), new String("bla"));
        assertAcked(
            indicesAdmin().prepareUpdateSettings(index1, index2)
                .setSettings(Settings.builder().putList("query.default_field", queryFieldsSetting))
        );
        final Settings updatedIndex1SettingsMaster = clusterServiceMaster.state().metadata().index(index1).getSettings();
        final Settings updatedIndex1SettingsData = clusterServiceData.state().metadata().index(index1).getSettings();
        assertNotSame(updatedIndex1SettingsMaster, updatedIndex1SettingsData);
        assertEqualsAndStringsInterned(queryFieldsSetting, updatedIndex1SettingsMaster);
        assertEqualsAndStringsInterned(queryFieldsSetting, updatedIndex1SettingsData);
        assertEqualsAndStringsInterned(queryFieldsSetting, clusterServiceMaster.state().metadata().index(index2).getSettings());
        assertEqualsAndStringsInterned(queryFieldsSetting, clusterServiceData.state().metadata().index(index2).getSettings());
    }

    private void assertEqualsAndStringsInterned(List<String> queryFieldsSetting, Settings settings) {
        final List<String> defaultFields = settings.getAsList("index.query.default_field");
        assertEquals(queryFieldsSetting, defaultFields);
        assertNotSame(queryFieldsSetting, defaultFields);
        // all setting strings should be interned
        assertSame("foo", defaultFields.get(0));
        assertSame("bar", defaultFields.get(1));
        assertSame("bla", defaultFields.get(2));
        for (String key : settings.keySet()) {
            assertSame(key, key.intern());
        }
    }

}
