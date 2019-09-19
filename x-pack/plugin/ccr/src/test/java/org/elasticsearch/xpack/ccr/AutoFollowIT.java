/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowerInfo;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
            assertTrue(ESIntegTestCase.indexExists("copy-logs-201901", followerClient()));
        });
        createLeaderIndex("transactions-201901", leaderIndexSettings);
        assertBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(2L));
            assertTrue(ESIntegTestCase.indexExists("copy-transactions-201901", followerClient()));
        });

        assertFalse(ESIntegTestCase.indexExists("copy-metrics-201901", followerClient()));
        assertFalse(ESIntegTestCase.indexExists("copy-logs-201812", followerClient()));
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

            assertTrue(ESIntegTestCase.indexExists("copy-logs-201901", followerClient()));

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
        long numIndices = randomIntBetween(4, 8);
        for (int i = 0; i < numIndices; i++) {
            createLeaderIndex("logs-" + i, leaderIndexSettings);
        }
        long expectedVal1 = numIndices;
        MetaData[] metaData = new MetaData[1];
        AutoFollowStats[] autoFollowStats = new AutoFollowStats[1];
        try {
            assertBusy(() -> {
                metaData[0] = getFollowerCluster().clusterService().state().metaData();
                autoFollowStats[0] = getAutoFollowStats();

                assertThat(metaData[0].indices().size(), equalTo((int) expectedVal1));
                AutoFollowMetadata autoFollowMetadata = metaData[0].custom(AutoFollowMetadata.TYPE);
                assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern"), hasSize((int) expectedVal1));
                assertThat(autoFollowStats[0].getNumberOfSuccessfulFollowIndices(), equalTo(expectedVal1));
            }, 30, TimeUnit.SECONDS);
        } catch (AssertionError ae) {
            logger.warn("indices={}", Arrays.toString(metaData[0].indices().keys().toArray(String.class)));
            logger.warn("auto follow stats={}", Strings.toString(autoFollowStats[0]));
            throw ae;
        }

        // Delete auto follow pattern and make sure that in the background the auto follower has stopped
        // then the leader index created after that should never be auto followed:
        deleteAutoFollowPatternSetting();
        try {
            assertBusy(() -> {
                metaData[0] = getFollowerCluster().clusterService().state().metaData();
                autoFollowStats[0] = getAutoFollowStats();

                assertThat(metaData[0].indices().size(), equalTo((int )expectedVal1));
                AutoFollowMetadata autoFollowMetadata = metaData[0].custom(AutoFollowMetadata.TYPE);
                assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern"), nullValue());
                assertThat(autoFollowStats[0].getAutoFollowedClusters().size(), equalTo(0));
            }, 30, TimeUnit.SECONDS);
        } catch (AssertionError ae) {
            logger.warn("indices={}", Arrays.toString(metaData[0].indices().keys().toArray(String.class)));
            logger.warn("auto follow stats={}", Strings.toString(autoFollowStats[0]));
            throw ae;
        }
        createLeaderIndex("logs-does-not-count", leaderIndexSettings);

        putAutoFollowPatterns("my-pattern", new String[] {"logs-*"});
        long i = numIndices;
        numIndices = numIndices + randomIntBetween(4, 8);
        for (; i < numIndices; i++) {
            createLeaderIndex("logs-" + i, leaderIndexSettings);
        }
        long expectedVal2 = numIndices;

        try {
            assertBusy(() -> {
                metaData[0] = getFollowerCluster().clusterService().state().metaData();
                autoFollowStats[0] = getAutoFollowStats();

                assertThat(metaData[0].indices().size(), equalTo((int) expectedVal2));
                AutoFollowMetadata autoFollowMetadata = metaData[0].custom(AutoFollowMetadata.TYPE);
                // expectedVal2 + 1, because logs-does-not-count is also marked as auto followed.
                // (This is because indices created before a pattern exists are not auto followed and are just marked as such.)
                assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern"), hasSize((int) expectedVal2 + 1));
                long count = Arrays.stream(metaData[0].getConcreteAllIndices()).filter(s -> s.startsWith("copy-")).count();
                assertThat(count, equalTo(expectedVal2));
                // Ensure that there are no auto follow errors:
                // (added specifically to see that there are no leader indices auto followed multiple times)
                assertThat(autoFollowStats[0].getRecentAutoFollowErrors().size(), equalTo(0));
            }, 30, TimeUnit.SECONDS);
        } catch (AssertionError ae) {
            logger.warn("indices={}", Arrays.toString(metaData[0].indices().keys().toArray(String.class)));
            logger.warn("auto follow stats={}", Strings.toString(autoFollowStats[0]));
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
        request.setRemoteCluster("leader_cluster");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));
        // Need to set this, because following an index in the same cluster
        request.setFollowIndexNamePattern("copy-{{leader_index}}");
        if (randomBoolean()) {
            request.getParameters().setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.getParameters().setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.getParameters().setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.getParameters().setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.getParameters().setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES));
        }
        if (randomBoolean()) {
            request.getParameters().setMaxRetryDelay(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.getParameters().setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.getParameters().setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES));
        }
        if (randomBoolean()) {
            request.getParameters().setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }

        request.setName("my-pattern");
        assertTrue(followerClient().execute(PutAutoFollowPatternAction.INSTANCE, request).actionGet().isAcknowledged());

        createLeaderIndex("logs-201901", leaderIndexSettings);
        assertBusy(() -> {
            FollowInfoAction.Request followInfoRequest = new FollowInfoAction.Request();
            followInfoRequest.setFollowerIndices("copy-logs-201901");
            FollowInfoAction.Response followInfoResponse;
            try {
                 followInfoResponse = followerClient().execute(FollowInfoAction.INSTANCE, followInfoRequest).actionGet();
            } catch (IndexNotFoundException e) {
                throw new AssertionError(e);
            }

            assertThat(followInfoResponse.getFollowInfos().size(), equalTo(1));
            FollowerInfo followerInfo = followInfoResponse.getFollowInfos().get(0);
            assertThat(followerInfo.getFollowerIndex(), equalTo("copy-logs-201901"));
            assertThat(followerInfo.getRemoteCluster(), equalTo("leader_cluster"));
            assertThat(followerInfo.getLeaderIndex(), equalTo("logs-201901"));

            FollowParameters followParameters = followerInfo.getParameters();
            assertThat(followParameters, notNullValue());
            if (request.getParameters().getMaxWriteBufferCount() != null) {
                assertThat(followParameters.getMaxWriteBufferCount(), equalTo(request.getParameters().getMaxWriteBufferCount()));
            }
            if (request.getParameters().getMaxWriteBufferSize() != null) {
                assertThat(followParameters.getMaxWriteBufferSize(), equalTo(request.getParameters().getMaxWriteBufferSize()));
            }
            if (request.getParameters().getMaxOutstandingReadRequests() != null) {
                assertThat(followParameters.getMaxOutstandingReadRequests(),
                    equalTo(request.getParameters().getMaxOutstandingReadRequests()));
            }
            if (request.getParameters().getMaxOutstandingWriteRequests() != null) {
                assertThat(followParameters.getMaxOutstandingWriteRequests(),
                    equalTo(request.getParameters().getMaxOutstandingWriteRequests()));
            }
            if (request.getParameters().getMaxReadRequestOperationCount() != null) {
                assertThat(followParameters.getMaxReadRequestOperationCount(),
                    equalTo(request.getParameters().getMaxReadRequestOperationCount()));
            }
            if (request.getParameters().getMaxReadRequestSize() != null) {
                assertThat(followParameters.getMaxReadRequestSize(), equalTo(request.getParameters().getMaxReadRequestSize()));
            }
            if (request.getParameters().getMaxRetryDelay() != null) {
                assertThat(followParameters.getMaxRetryDelay(), equalTo(request.getParameters().getMaxRetryDelay()));
            }
            if (request.getParameters().getReadPollTimeout() != null) {
                assertThat(followParameters.getReadPollTimeout(), equalTo(request.getParameters().getReadPollTimeout()));
            }
            if (request.getParameters().getMaxWriteRequestOperationCount() != null) {
                assertThat(followParameters.getMaxWriteRequestOperationCount(),
                    equalTo(request.getParameters().getMaxWriteRequestOperationCount()));
            }
            if (request.getParameters().getMaxWriteRequestSize() != null) {
                assertThat(followParameters.getMaxWriteRequestSize(), equalTo(request.getParameters().getMaxWriteRequestSize()));
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
        assertTrue(ESIntegTestCase.indexExists("copy-logs-201701", followerClient()));

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

        assertFalse(ESIntegTestCase.indexExists("copy-logs-201801", followerClient()));
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
            assertFalse(ESIntegTestCase.indexExists("copy-logs-20200101", followerClient()));
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
            assertTrue(ESIntegTestCase.indexExists("copy-logs-20200102", followerClient()));
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
