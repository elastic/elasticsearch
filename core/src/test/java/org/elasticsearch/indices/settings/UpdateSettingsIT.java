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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MergeSchedulerConfig;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class UpdateSettingsIT extends ESIntegTestCase {

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
        return Arrays.asList(DummySettingPlugin.class);
    }

    public static class DummySettingPlugin extends Plugin {
        public static final Setting<String> DUMMY_SETTING = Setting.simpleString("index.dummy",
            Setting.Property.IndexScope, Setting.Property.Dynamic);
        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSettingsUpdateConsumer(DUMMY_SETTING, (s) -> {}, (s) -> {
                if (s.equals("boom"))
                    throw new IllegalArgumentException("this setting goes boom");
            });
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(DUMMY_SETTING);
        }
    }

    public void testResetDefault() {
        createIndex("test");

        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.refresh_interval", -1).put("index.translog.flush_threshold_size", "1024b"))
            .execute()
            .actionGet();
        IndexMetaData indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertEquals(indexMetaData.getSettings().get("index.refresh_interval"), "-1");
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), -1);
                assertEquals(indexService.getIndexSettings().getFlushThresholdSize().getBytes(), 1024);
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
            }
        }
    }
    public void testOpenCloseUpdateSettings() throws Exception {
        createIndex("test");
        try {
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder()
                    .put("index.refresh_interval", -1) // this one can change
                    .put("index.fielddata.cache", "none")) // this one can't
                .execute()
                .actionGet();
            fail();
        } catch (IllegalArgumentException e) {
            // all is well
        }

        IndexMetaData indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.getSettings().get("index.refresh_interval"), nullValue());
        assertThat(indexMetaData.getSettings().get("index.fielddata.cache"), nullValue());

        // Now verify via dedicated get settings api:
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), nullValue());
        assertThat(getSettingsResponse.getSetting("test", "index.fielddata.cache"), nullValue());

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

        // Now verify via dedicated get settings api:
        getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), equalTo("1s"));
        assertThat(getSettingsResponse.getSetting("test", "index.fielddata.cache"), equalTo("none"));
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

    private static class MockAppender extends AbstractAppender {
        public boolean sawUpdateMaxThreadCount;
        public boolean sawUpdateAutoThrottle;

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null);
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            if (event.getLevel() == Level.TRACE && event.getLoggerName().endsWith("lucene.iw")) {
            }
            if (event.getLevel() == Level.INFO
                && message.contains("updating [index.merge.scheduler.max_thread_count] from [10000] to [1]")) {
                sawUpdateMaxThreadCount = true;
            }
            if (event.getLevel() == Level.INFO
                && message.contains("updating [index.merge.scheduler.auto_throttle] from [true] to [false]")) {
                sawUpdateAutoThrottle = true;
            }
        }

        @Override
        public boolean ignoreExceptions() {
            return false;
        }

    }

    public void testUpdateAutoThrottleSettings() throws Exception {
        MockAppender mockAppender = new MockAppender("testUpdateAutoThrottleSettings");
        mockAppender.start();
        Logger rootLogger = LogManager.getRootLogger();
        Loggers.addAppender(rootLogger, mockAppender);
        Level savedLevel = rootLogger.getLevel();
        Loggers.setLevel(rootLogger, Level.TRACE);

        try {
            // No throttling at first, only 1 non-replicated shard, force lots of merging:
            assertAcked(prepareCreate("test")
                        .setSettings(Settings.builder()
                                     .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
                                     .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                                     .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), "2")
                                     .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), "2")
                                     .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
                                     .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "2")
                                     .put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), "true")));

            // Disable auto throttle:
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), "false"))
                .get();

            // if a node has processed the cluster state update but not yet returned from the update task, it might still log messages;
            // these log messages will race with the stopping of the appender so we wait to ensure these tasks are done processing
            assertBusy(() -> {
                for (final ClusterService service : internalCluster().getInstances(ClusterService.class)) {
                    assertThat(service.numberOfPendingTasks(), equalTo(0));
                }
            });

            // Make sure we log the change:
            assertTrue(mockAppender.sawUpdateAutoThrottle);

            // Make sure setting says it is in fact changed:
            GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
            assertThat(getSettingsResponse.getSetting("test", MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey()), equalTo("false"));
        } finally {
            Loggers.setLevel(rootLogger, savedLevel);
            Loggers.removeAppender(rootLogger, mockAppender);
            // don't call stop here some node might still use this reference at this point causing tests to fail.
            // this is only relevant in integ tests, unittest can control what uses a logger and what doesn't
            // mockAppender.stop();
        }
    }

    public void testInvalidMergeMaxThreadCount() throws IllegalAccessException {
        CreateIndexRequestBuilder createBuilder = prepareCreate("test")
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), "2")
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), "2")
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "100")
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "10")
            );
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> createBuilder.get());
        assertThat(exc.getMessage(), equalTo("maxThreadCount (= 100) should be <= maxMergeCount (= 10)"));

        assertAcked(prepareCreate("test")
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), "2")
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), "2")
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "100")
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "100")
            ));

        {
            UpdateSettingsRequestBuilder updateBuilder =
                client()
                    .admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1000"));
            exc = expectThrows(IllegalArgumentException.class,
                () -> updateBuilder.get());
            assertThat(exc.getMessage(), equalTo("maxThreadCount (= 1000) should be <= maxMergeCount (= 100)"));
        }

        {
            UpdateSettingsRequestBuilder updateBuilder =
                client()
                    .admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "10"));
            exc = expectThrows(IllegalArgumentException.class,
                () -> updateBuilder.get());
            assertThat(exc.getMessage(), equalTo("maxThreadCount (= 100) should be <= maxMergeCount (= 10)"));
        }
    }

    // #6882: make sure we can change index.merge.scheduler.max_thread_count live
    public void testUpdateMergeMaxThreadCount() throws Exception {
        MockAppender mockAppender = new MockAppender("testUpdateMergeMaxThreadCount");
        mockAppender.start();
        Logger rootLogger = LogManager.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        Loggers.addAppender(rootLogger, mockAppender);
        Loggers.setLevel(rootLogger, Level.TRACE);

        try {

            assertAcked(prepareCreate("test")
                        .setSettings(Settings.builder()
                                     .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
                                     .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                                     .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), "2")
                                     .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), "2")
                                     .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "10000")
                                     .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "10000")));

            assertFalse(mockAppender.sawUpdateMaxThreadCount);
            // Now make a live change to reduce allowed merge threads:
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1"))
                .get();

            // if a node has processed the cluster state update but not yet returned from the update task, it might still log messages;
            // these log messages will race with the stopping of the appender so we wait to ensure these tasks are done processing
            assertBusy(() -> {
                for (final ClusterService service : internalCluster().getInstances(ClusterService.class)) {
                    assertThat(service.numberOfPendingTasks(), equalTo(0));
                }
            });

            // Make sure we log the change:
            assertTrue(mockAppender.sawUpdateMaxThreadCount);

            // Make sure setting says it is in fact changed:
            GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
            assertThat(getSettingsResponse.getSetting("test", MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey()), equalTo("1"));

        } finally {
            Loggers.setLevel(rootLogger, savedLevel);
            Loggers.removeAppender(rootLogger, mockAppender);
            // don't call stop here some node might still use this reference at this point causing tests to fail.
            // this is only relevant in integ tests, unittest can control what uses a logger and what doesn't
            // mockAppender.stop();
        }
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
