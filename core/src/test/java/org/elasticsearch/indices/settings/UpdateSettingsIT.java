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

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.elasticsearch.index.shard.MergeSchedulerConfig;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.MergePolicyConfig;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class UpdateSettingsIT extends ESIntegTestCase {

    @Test
    public void testOpenCloseUpdateSettings() throws Exception {
        createIndex("test");
        try {
            client().admin().indices().prepareUpdateSettings("test")
                    .setSettings(Settings.settingsBuilder()
                            .put("index.refresh_interval", -1) // this one can change
                            .put("index.cache.filter.type", "none") // this one can't
                    )
                    .execute().actionGet();
            fail();
        } catch (IllegalArgumentException e) {
            // all is well
        }

        IndexMetaData indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.settings().get("index.refresh_interval"), nullValue());
        assertThat(indexMetaData.settings().get("index.cache.filter.type"), nullValue());

        // Now verify via dedicated get settings api:
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), nullValue());
        assertThat(getSettingsResponse.getSetting("test", "index.cache.filter.type"), nullValue());

        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.settingsBuilder()
                        .put("index.refresh_interval", -1) // this one can change
                )
                .execute().actionGet();

        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.settings().get("index.refresh_interval"), equalTo("-1"));
        // Now verify via dedicated get settings api:
        getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), equalTo("-1"));

        // now close the index, change the non dynamic setting, and see that it applies

        // Wait for the index to turn green before attempting to close it
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setTimeout("30s").setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        client().admin().indices().prepareClose("test").execute().actionGet();

        try {
            client().admin().indices().prepareUpdateSettings("test")
                    .setSettings(Settings.settingsBuilder()
                                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                    )
                    .execute().actionGet();
            fail("can't change number of replicas on a closed index");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "Can't update [index.number_of_replicas] on closed indices [[test]] - can leave index in an unopenable state");
            // expected
        }
        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.settingsBuilder()
                        .put("index.refresh_interval", "1s") // this one can change
                        .put("index.cache.filter.type", "none") // this one can't
                )
                .execute().actionGet();

        indexMetaData = client().admin().cluster().prepareState().execute().actionGet().getState().metaData().index("test");
        assertThat(indexMetaData.settings().get("index.refresh_interval"), equalTo("1s"));
        assertThat(indexMetaData.settings().get("index.cache.filter.type"), equalTo("none"));

        // Now verify via dedicated get settings api:
        getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), equalTo("1s"));
        assertThat(getSettingsResponse.getSetting("test", "index.cache.filter.type"), equalTo("none"));
    }

    @Test
    public void testEngineGCDeletesSetting() throws InterruptedException {
        createIndex("test");
        client().prepareIndex("test", "type", "1").setSource("f", 1).get(); // set version to 1
        client().prepareDelete("test", "type", "1").get(); // sets version to 2
        client().prepareIndex("test", "type", "1").setSource("f", 2).setVersion(2).get(); // delete is still in cache this should work & set version to 3
        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.settingsBuilder()
                        .put("index.gc_deletes", 0)
                ).get();

        client().prepareDelete("test", "type", "1").get(); // sets version to 4
        Thread.sleep(300); // wait for cache time to change TODO: this needs to be solved better. To be discussed.
        assertThrows(client().prepareIndex("test", "type", "1").setSource("f", 3).setVersion(4), VersionConflictEngineException.class); // delete is should not be in cache

    }

    // #6626: make sure we can update throttle settings and the changes take effect
    @Test
    public void testUpdateThrottleSettings() {

        // No throttling at first, only 1 non-replicated shard, force lots of merging:
        assertAcked(prepareCreate("test")
                    .setSettings(Settings.builder()
                                 .put(IndexStore.INDEX_STORE_THROTTLE_TYPE, "none")
                                 .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
                                 .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                                 .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE, "2")
                                 .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER, "2")
                                 .put(MergeSchedulerConfig.MAX_THREAD_COUNT, "1")
                                 .put(MergeSchedulerConfig.MAX_MERGE_COUNT, "2")
                                 .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL, 0) // get stats all the time - no caching
                                 ));
        ensureGreen();
        long termUpto = 0;
        for(int i=0;i<100;i++) {
            // Provoke slowish merging by making many unique terms:
            StringBuilder sb = new StringBuilder();
            for(int j=0;j<100;j++) {
                sb.append(' ');
                sb.append(termUpto++);
            }
            client().prepareIndex("test", "type", ""+termUpto).setSource("field" + (i%10), sb.toString()).get();
            if (i % 2 == 0) {
                refresh();
            }
        }

        // No merge IO throttling should have happened:
        NodesStatsResponse nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).get();
        for(NodeStats stats : nodesStats.getNodes()) {
            assertThat(stats.getIndices().getStore().getThrottleTime().getMillis(), equalTo(0l));
        }

        logger.info("test: set low merge throttling");

        // Now updates settings to turn on merge throttling lowish rate
        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder()
                         .put(IndexStore.INDEX_STORE_THROTTLE_TYPE, "merge")
                         .put(IndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, "1mb"))
            .get();

        // Make sure setting says it is in fact changed:
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", IndexStore.INDEX_STORE_THROTTLE_TYPE), equalTo("merge"));

        // Also make sure we see throttling kicking in:
        boolean done = false;
        while (done == false) {
            // Provoke slowish merging by making many unique terms:
            for(int i=0;i<5;i++) {
                StringBuilder sb = new StringBuilder();
                for(int j=0;j<100;j++) {
                    sb.append(' ');
                    sb.append(termUpto++);
                    sb.append(" some random text that keeps repeating over and over again hambone");
                }
                client().prepareIndex("test", "type", ""+termUpto).setSource("field" + (i%10), sb.toString()).get();
            }
            refresh();
            nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).get();
            for(NodeStats stats : nodesStats.getNodes()) {
                long throttleMillis = stats.getIndices().getStore().getThrottleTime().getMillis();
                if (throttleMillis > 0) {
                    done = true;
                    break;
                }
            }
        }

        logger.info("test: disable merge throttling");
        
        // Now updates settings to disable merge throttling
        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder()
                         .put(IndexStore.INDEX_STORE_THROTTLE_TYPE, "none"))
            .get();

        // Optimize does a waitForMerges, which we must do to make sure all in-flight (throttled) merges finish:
        logger.info("test: optimize");
        client().admin().indices().prepareOptimize("test").setMaxNumSegments(1).get();
        logger.info("test: optimize done");

        // Record current throttling so far
        long sumThrottleTime = 0;
        nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).get();
        for(NodeStats stats : nodesStats.getNodes()) {
            sumThrottleTime += stats.getIndices().getStore().getThrottleTime().getMillis();
        }

        // Make sure no further throttling happens:
        for(int i=0;i<100;i++) {
            // Provoke slowish merging by making many unique terms:
            StringBuilder sb = new StringBuilder();
            for(int j=0;j<100;j++) {
                sb.append(' ');
                sb.append(termUpto++);
            }
            client().prepareIndex("test", "type", ""+termUpto).setSource("field" + (i%10), sb.toString()).get();
            if (i % 2 == 0) {
                refresh();
            }
        }
        logger.info("test: done indexing after disabling throttling");

        long newSumThrottleTime = 0;
        nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).get();
        for(NodeStats stats : nodesStats.getNodes()) {
            newSumThrottleTime += stats.getIndices().getStore().getThrottleTime().getMillis();
        }

        // No additional merge IO throttling should have happened:
        assertEquals(sumThrottleTime, newSumThrottleTime);

        // Optimize & flush and wait; else we sometimes get a "Delete Index failed - not acked"
        // when ESIntegTestCase.after tries to remove indices created by the test:

        // Wait for merges to finish
        client().admin().indices().prepareOptimize("test").get();
        flush();

        logger.info("test: test done");
    }

    private static class MockAppender extends AppenderSkeleton {
        public boolean sawIndexWriterMessage;
        public boolean sawFlushDeletes;
        public boolean sawMergeThreadPaused;
        public boolean sawUpdateMaxThreadCount;
        public boolean sawUpdateAutoThrottle;

        @Override
        protected void append(LoggingEvent event) {
            String message = event.getMessage().toString();
            if (event.getLevel() == Level.TRACE &&
                event.getLoggerName().endsWith("lucene.iw")) {
                sawFlushDeletes |= message.contains("IW: apply all deletes during flush");
                sawMergeThreadPaused |= message.contains("CMS: pause thread");
            }
            if (event.getLevel() == Level.INFO && message.contains("updating [index.merge.scheduler.max_thread_count] from [10000] to [1]")) {
                sawUpdateMaxThreadCount = true;
            }
            if (event.getLevel() == Level.INFO && message.contains("updating [index.merge.scheduler.auto_throttle] from [true] to [false]")) {
                sawUpdateAutoThrottle = true;
            }
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testUpdateAutoThrottleSettings() {

        MockAppender mockAppender = new MockAppender();
        Logger rootLogger = Logger.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        rootLogger.addAppender(mockAppender);
        rootLogger.setLevel(Level.TRACE);

        try {
            // No throttling at first, only 1 non-replicated shard, force lots of merging:
            assertAcked(prepareCreate("test")
                        .setSettings(Settings.builder()
                                     .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
                                     .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                                     .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE, "2")
                                     .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER, "2")
                                     .put(MergeSchedulerConfig.MAX_THREAD_COUNT, "1")
                                     .put(MergeSchedulerConfig.MAX_MERGE_COUNT, "2")
                                     .put(MergeSchedulerConfig.AUTO_THROTTLE, "true")
                                     ));

            // Disable auto throttle:
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder()
                             .put(MergeSchedulerConfig.AUTO_THROTTLE, "no"))
                .get();

            // Make sure we log the change:
            assertTrue(mockAppender.sawUpdateAutoThrottle);

            // Make sure setting says it is in fact changed:
            GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
            assertThat(getSettingsResponse.getSetting("test", MergeSchedulerConfig.AUTO_THROTTLE), equalTo("no"));
        } finally {
            rootLogger.removeAppender(mockAppender);
            rootLogger.setLevel(savedLevel);
        }
    }

    // #6882: make sure we can change index.merge.scheduler.max_thread_count live
    @Test
    public void testUpdateMergeMaxThreadCount() {

        MockAppender mockAppender = new MockAppender();
        Logger rootLogger = Logger.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        rootLogger.addAppender(mockAppender);
        rootLogger.setLevel(Level.TRACE);

        try {

            assertAcked(prepareCreate("test")
                        .setSettings(Settings.builder()
                                     .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
                                     .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                                     .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE, "2")
                                     .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER, "2")
                                     .put(MergeSchedulerConfig.MAX_THREAD_COUNT, "10000")
                                     .put(MergeSchedulerConfig.MAX_MERGE_COUNT, "10000")
                                     ));

            assertFalse(mockAppender.sawUpdateMaxThreadCount);

            // Now make a live change to reduce allowed merge threads:
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder()
                             .put(MergeSchedulerConfig.MAX_THREAD_COUNT, "1")
                             )
                .get();
            
            // Make sure we log the change:
            assertTrue(mockAppender.sawUpdateMaxThreadCount);

            // Make sure setting says it is in fact changed:
            GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
            assertThat(getSettingsResponse.getSetting("test", MergeSchedulerConfig.MAX_THREAD_COUNT), equalTo("1"));

        } finally {
            rootLogger.removeAppender(mockAppender);
            rootLogger.setLevel(savedLevel);
        }
    }

    @Test
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
