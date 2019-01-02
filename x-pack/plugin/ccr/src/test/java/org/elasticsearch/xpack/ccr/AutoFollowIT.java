/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class AutoFollowIT extends CcrIntegTestCase {

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testAutoFollow() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        createLeaderIndex("logs-201812", leaderIndexSettings);

        // Enabling auto following:
        if (randomBoolean()) {
            putAutoFollowPatterns("my-pattern", new String[] {"logs-*", "transactions-*"});
        } else {
            putAutoFollowPatterns("my-pattern1", new String[] {"logs-*"});
            putAutoFollowPatterns("my-pattern2", new String[] {"transactions-*"});
        }

        createLeaderIndex("metrics-201901", leaderIndexSettings);

        createLeaderIndex("logs-201901", leaderIndexSettings);
        assertBusy(() -> {
            IndicesExistsRequest request = new IndicesExistsRequest("copy-logs-201901");
            assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());
        });
        createLeaderIndex("transactions-201901", leaderIndexSettings);
        assertBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(2L));

            IndicesExistsRequest request = new IndicesExistsRequest("copy-transactions-201901");
            assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());
        });

        IndicesExistsRequest request = new IndicesExistsRequest("copy-metrics-201901");
        assertFalse(followerClient().admin().indices().exists(request).actionGet().isExists());
        request = new IndicesExistsRequest("copy-logs-201812");
        assertFalse(followerClient().admin().indices().exists(request).actionGet().isExists());
    }

    public void testCleanFollowedLeaderIndexUUIDs() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        putAutoFollowPatterns("my-pattern", new String[] {"logs-*"});
        createLeaderIndex("logs-201901", leaderIndexSettings);
        assertBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));

            IndicesExistsRequest request = new IndicesExistsRequest("copy-logs-201901");
            assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());

            MetaData metaData = getFollowerCluster().clusterService().state().metaData();
            String leaderIndexUUID = metaData.index("copy-logs-201901")
                .getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY)
                .get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
            AutoFollowMetadata autoFollowMetadata = metaData.custom(AutoFollowMetadata.TYPE);
            assertThat(autoFollowMetadata, notNullValue());
            List<String> followedLeaderIndixUUIDs = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern");
            assertThat(followedLeaderIndixUUIDs.size(), equalTo(1));
            assertThat(followedLeaderIndixUUIDs.get(0), equalTo(leaderIndexUUID));
        });

        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("logs-201901");
        assertAcked(leaderClient().admin().indices().delete(deleteIndexRequest).actionGet());

        assertBusy(() -> {
            AutoFollowMetadata autoFollowMetadata = getFollowerCluster().clusterService().state()
                .metaData()
                .custom(AutoFollowMetadata.TYPE);
            assertThat(autoFollowMetadata, notNullValue());
            List<String> followedLeaderIndixUUIDs = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern");
            assertThat(followedLeaderIndixUUIDs.size(), equalTo(0));
        });
    }

    public void testAutoFollowManyIndices() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        putAutoFollowPatterns("my-pattern", new String[] {"logs-*"});
        int numIndices = randomIntBetween(4, 32);
        for (int i = 0; i < numIndices; i++) {
            createLeaderIndex("logs-" + i, leaderIndexSettings);
        }
        int expectedVal1 = numIndices;
        assertBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo((long) expectedVal1));
        });

        deleteAutoFollowPatternSetting();
        createLeaderIndex("logs-does-not-count", leaderIndexSettings);

        putAutoFollowPatterns("my-pattern", new String[] {"logs-*"});
        int i = numIndices;
        numIndices = numIndices + randomIntBetween(4, 32);
        for (; i < numIndices; i++) {
            createLeaderIndex("logs-" + i, leaderIndexSettings);
        }
        int expectedVal2 = numIndices;

        MetaData[] metaData = new MetaData[1];
        try {
            assertBusy(() -> {
                metaData[0] = followerClient().admin().cluster().prepareState().get().getState().metaData();
                int count = (int) Arrays.stream(metaData[0].getConcreteAllIndices()).filter(s -> s.startsWith("copy-")).count();
                assertThat(count, equalTo(expectedVal2));
            });
        } catch (AssertionError ae) {
            logger.warn("metadata={}", Strings.toString(metaData[0]));
            logger.warn("auto follow stats={}", Strings.toString(getAutoFollowStats()));
            throw ae;
        }
    }

    public void testAutoFollowParameterAreDelegated() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        // Enabling auto following:
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setName("my-pattern");
        request.setRemoteCluster("leader_cluster");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));
        // Need to set this, because following an index in the same cluster
        request.setFollowIndexNamePattern("copy-{{leader_index}}");
        if (randomBoolean()) {
            request.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxConcurrentReadBatches(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxConcurrentWriteBatches(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES));
        }
        if (randomBoolean()) {
            request.setMaxRetryDelay(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.setReadPollTimeout(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES));
        }
        if (randomBoolean()) {
            request.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        assertTrue(followerClient().execute(PutAutoFollowPatternAction.INSTANCE, request).actionGet().isAcknowledged());

        createLeaderIndex("logs-201901", leaderIndexSettings);
        assertBusy(() -> {
            PersistentTasksCustomMetaData persistentTasksMetaData =
                followerClient().admin().cluster().prepareState().get().getState().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertThat(persistentTasksMetaData, notNullValue());
            assertThat(persistentTasksMetaData.tasks().size(), equalTo(1));
            ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTasksMetaData.tasks().iterator().next().getParams();
            assertThat(shardFollowTask.getLeaderShardId().getIndexName(), equalTo("logs-201901"));
            assertThat(shardFollowTask.getFollowShardId().getIndexName(), equalTo("copy-logs-201901"));
            if (request.getMaxWriteBufferCount() != null) {
                assertThat(shardFollowTask.getMaxWriteBufferCount(), equalTo(request.getMaxWriteBufferCount()));
            }
            if (request.getMaxWriteBufferSize() != null) {
                assertThat(shardFollowTask.getMaxWriteBufferSize(), equalTo(request.getMaxWriteBufferSize()));
            }
            if (request.getMaxConcurrentReadBatches() != null) {
                assertThat(shardFollowTask.getMaxOutstandingReadRequests(), equalTo(request.getMaxConcurrentReadBatches()));
            }
            if (request.getMaxConcurrentWriteBatches() != null) {
                assertThat(shardFollowTask.getMaxOutstandingWriteRequests(), equalTo(request.getMaxConcurrentWriteBatches()));
            }
            if (request.getMaxReadRequestOperationCount() != null) {
                assertThat(shardFollowTask.getMaxReadRequestOperationCount(), equalTo(request.getMaxReadRequestOperationCount()));
            }
            if (request.getMaxReadRequestSize() != null) {
                assertThat(shardFollowTask.getMaxReadRequestSize(), equalTo(request.getMaxReadRequestSize()));
            }
            if (request.getMaxRetryDelay() != null) {
                assertThat(shardFollowTask.getMaxRetryDelay(), equalTo(request.getMaxRetryDelay()));
            }
            if (request.getReadPollTimeout() != null) {
                assertThat(shardFollowTask.getReadPollTimeout(), equalTo(request.getReadPollTimeout()));
            }
            if (request.getMaxWriteRequestOperationCount() != null) {
                assertThat(shardFollowTask.getMaxWriteRequestOperationCount(), equalTo(request.getMaxWriteRequestOperationCount()));
            }
            if (request.getMaxWriteRequestSize() != null) {
                assertThat(shardFollowTask.getMaxWriteRequestSize(), equalTo(request.getMaxWriteRequestSize()));
            }
        });
    }

    public void testConflictingPatterns() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        // Enabling auto following:
        putAutoFollowPatterns("my-pattern1", new String[] {"logs-*"});
        putAutoFollowPatterns("my-pattern2", new String[] {"logs-2018*"});

        createLeaderIndex("logs-201701", leaderIndexSettings);
        assertBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));
            assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(0L));
            assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(0L));
        });
        IndicesExistsRequest request = new IndicesExistsRequest("copy-logs-201701");
        assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());

        createLeaderIndex("logs-201801", leaderIndexSettings);
        assertBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));
            assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), greaterThanOrEqualTo(1L));
            assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(0L));

            assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(2));
            ElasticsearchException autoFollowError1 = autoFollowStats.getRecentAutoFollowErrors().get("my-pattern1:logs-201801").v2();
            assertThat(autoFollowError1, notNullValue());
            assertThat(autoFollowError1.getRootCause().getMessage(), equalTo("index to follow [logs-201801] for pattern [my-pattern1] " +
                "matches with other patterns [my-pattern2]"));

            ElasticsearchException autoFollowError2 = autoFollowStats.getRecentAutoFollowErrors().get("my-pattern2:logs-201801").v2();
            assertThat(autoFollowError2, notNullValue());
            assertThat(autoFollowError2.getRootCause().getMessage(), equalTo("index to follow [logs-201801] for pattern [my-pattern2] " +
                "matches with other patterns [my-pattern1]"));
        });

        request = new IndicesExistsRequest("copy-logs-201801");
        assertFalse(followerClient().admin().indices().exists(request).actionGet().isExists());
    }

    public void testAutoFollowSoftDeletesDisabled() throws Exception {
        putAutoFollowPatterns("my-pattern1", new String[] {"logs-*"});

        // Soft deletes are disabled:
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false)
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
        createLeaderIndex("logs-20200101", leaderIndexSettings);
        assertBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
            assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(1L));
            assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(1));
            ElasticsearchException failure  = autoFollowStats.getRecentAutoFollowErrors().firstEntry().getValue().v2();
            assertThat(failure.getMessage(), equalTo("index [logs-20200101] cannot be followed, " +
                "because soft deletes are not enabled"));
            IndicesExistsRequest request = new IndicesExistsRequest("copy-logs-20200101");
            assertFalse(followerClient().admin().indices().exists(request).actionGet().isExists());
        });

        // Soft deletes are enabled:
        leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
        createLeaderIndex("logs-20200102", leaderIndexSettings);
        assertBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));
            IndicesExistsRequest request = new IndicesExistsRequest("copy-logs-20200102");
            assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());
        });
    }

    private void putAutoFollowPatterns(String name, String[] patterns) {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setName(name);
        request.setRemoteCluster("leader_cluster");
        request.setLeaderIndexPatterns(Arrays.asList(patterns));
        // Need to set this, because following an index in the same cluster
        request.setFollowIndexNamePattern("copy-{{leader_index}}");
        assertTrue(followerClient().execute(PutAutoFollowPatternAction.INSTANCE, request).actionGet().isAcknowledged());
    }

    private void deleteAutoFollowPatternSetting() {
        DeleteAutoFollowPatternAction.Request request = new DeleteAutoFollowPatternAction.Request("my-pattern");
        assertTrue(followerClient().execute(DeleteAutoFollowPatternAction.INSTANCE, request).actionGet().isAcknowledged());
    }

    private AutoFollowStats getAutoFollowStats() {
        CcrStatsAction.Request request = new CcrStatsAction.Request();
        return followerClient().execute(CcrStatsAction.INSTANCE, request).actionGet().getAutoFollowStats();
    }

    private void createLeaderIndex(String index, Settings settings) {
        CreateIndexRequest request = new CreateIndexRequest(index);
        request.settings(settings);
        leaderClient().admin().indices().create(request).actionGet();
    }

}
