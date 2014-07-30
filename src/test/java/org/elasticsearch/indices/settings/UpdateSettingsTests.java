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
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.merge.policy.TieredMergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.ConcurrentMergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerModule;
import org.elasticsearch.index.store.support.AbstractIndexStore;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class UpdateSettingsTests extends ElasticsearchIntegrationTest {

    @Test
    public void testOpenCloseUpdateSettings() throws Exception {
        createIndex("test");
        try {
            client().admin().indices().prepareUpdateSettings("test")
                    .setSettings(ImmutableSettings.settingsBuilder()
                            .put("index.refresh_interval", -1) // this one can change
                            .put("index.cache.filter.type", "none") // this one can't
                    )
                    .execute().actionGet();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {
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
                .setSettings(ImmutableSettings.settingsBuilder()
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

        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.settingsBuilder()
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
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("index.gc_deletes", 0)
                ).get();

        client().prepareDelete("test", "type", "1").get(); // sets version to 4
        Thread.sleep(300); // wait for cache time to change TODO: this needs to be solved better. To be discussed.
        assertThrows(client().prepareIndex("test", "type", "1").setSource("f", 3).setVersion(4), VersionConflictEngineException.class); // delete is should not be in cache

    }

    // #6626: make sure we can update throttle settings and the changes take effect
    @Test
    @Slow
    public void testUpdateThrottleSettings() {

        // No throttling at first, only 1 non-replicated shard, force lots of merging:
        assertAcked(prepareCreate("test")
                    .setSettings(ImmutableSettings.builder()
                                 .put(AbstractIndexStore.INDEX_STORE_THROTTLE_TYPE, "none")
                                 .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
                                 .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                                 .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE, "2")
                                 .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER, "2")
                                 .put(ConcurrentMergeSchedulerProvider.MAX_THREAD_COUNT, "1")
                                 .put(ConcurrentMergeSchedulerProvider.MAX_MERGE_COUNT, "2")
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

        // Now updates settings to turn on merge throttling lowish rate
        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(ImmutableSettings.builder()
                         .put(AbstractIndexStore.INDEX_STORE_THROTTLE_TYPE, "merge")
                         .put(AbstractIndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, "1mb"))
            .get();

        // Make sure setting says it is in fact changed:
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", AbstractIndexStore.INDEX_STORE_THROTTLE_TYPE), equalTo("merge"));

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

        // Now updates settings to disable merge throttling
        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(ImmutableSettings.builder()
                         .put(AbstractIndexStore.INDEX_STORE_THROTTLE_TYPE, "none"))
            .get();

        // Optimize does a waitForMerges, which we must do to make sure all in-flight (throttled) merges finish:
        client().admin().indices().prepareOptimize("test").get();

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

        long newSumThrottleTime = 0;
        nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).get();
        for(NodeStats stats : nodesStats.getNodes()) {
            newSumThrottleTime += stats.getIndices().getStore().getThrottleTime().getMillis();
        }

        // No additional merge IO throttling should have happened:
        assertEquals(sumThrottleTime, newSumThrottleTime);
    }

    private static class MockAppender extends AppenderSkeleton {
        public boolean sawIndexWriterMessage;
        public boolean sawFlushDeletes;
        public boolean sawMergeThreadPaused;
        public boolean sawUpdateSetting;

        @Override
        protected void append(LoggingEvent event) {
            String message = event.getMessage().toString();
            if (event.getLevel() == Level.TRACE &&
                event.getLoggerName().endsWith("lucene.iw")) {
                sawFlushDeletes |= message.contains("IW: apply all deletes during flush");
                sawMergeThreadPaused |= message.contains("CMS: pause thread");
            }
            if (event.getLevel() == Level.INFO && message.contains("updating [max_thread_count] from [10000] to [1]")) {
                sawUpdateSetting = true;
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

    // #6882: make sure we can change index.merge.scheduler.max_thread_count live
    @Test
    @Slow
    public void testUpdateMergeMaxThreadCount() {

        MockAppender mockAppender = new MockAppender();
        Logger rootLogger = Logger.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        rootLogger.addAppender(mockAppender);
        rootLogger.setLevel(Level.TRACE);

        try {

            // Tons of merge threads allowed, only 1 non-replicated shard, force lots of merging, throttle so they fall behind:
            assertAcked(prepareCreate("test")
                        .setSettings(ImmutableSettings.builder()
                                     .put(AbstractIndexStore.INDEX_STORE_THROTTLE_TYPE, "merge")
                                     .put(AbstractIndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, "1mb")
                                     .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
                                     .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                                     .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE, "2")
                                     .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER, "2")
                                     .put(MergeSchedulerModule.MERGE_SCHEDULER_TYPE_KEY, ConcurrentMergeSchedulerProvider.class)
                                     .put(ConcurrentMergeSchedulerProvider.MAX_THREAD_COUNT, "10000")
                                     .put(ConcurrentMergeSchedulerProvider.MAX_MERGE_COUNT, "10000")
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

            assertTrue(mockAppender.sawFlushDeletes);
            assertFalse(mockAppender.sawMergeThreadPaused);
            mockAppender.sawFlushDeletes = false;
            mockAppender.sawMergeThreadPaused = false;

            assertFalse(mockAppender.sawUpdateSetting);

            // Now make a live change to reduce allowed merge threads, and waaay over-throttle merging so they fall behind:
            client()
                .admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.builder()
                             .put(ConcurrentMergeSchedulerProvider.MAX_THREAD_COUNT, "1")
                             .put(AbstractIndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, "10kb")
                             )
                .get();

            try {

                // Make sure we log the change:
                assertTrue(mockAppender.sawUpdateSetting);

                int i = 0;
                while (true) {
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
                    // This time we should see some merges were in fact paused:
                    if (mockAppender.sawMergeThreadPaused) {
                        break;
                    }
                    i++;
                }
            } finally {
                // Make merges fast again & finish merges before we try to close; else we sometimes get a "Delete Index failed - not acked"
                // when ElasticsearchIntegrationTest.after tries to remove indices created by the test:
                client()
                    .admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(ImmutableSettings.builder()
                                 .put(ConcurrentMergeSchedulerProvider.MAX_THREAD_COUNT, "3")
                                 .put(AbstractIndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, "20mb")
                                 )
                    .get();

                // Wait for merges to finish
                client().admin().indices().prepareOptimize("test").setWaitForMerge(true).get();
            }


        } finally {
            rootLogger.removeAppender(mockAppender);
            rootLogger.setLevel(savedLevel);
        }
    }
}
