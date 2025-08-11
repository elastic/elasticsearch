/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.CcrAutoFollowInfoFetcher;
import org.elasticsearch.xpack.core.ccr.CcrConstants;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowerInfo;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AutoFollowIT extends CcrIntegTestCase {

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(FakeSystemIndexPlugin.class, SecondFakeSystemIndexPlugin.class, DataStreamsPlugin.class)
        ).collect(Collectors.toList());
    }

    public static class FakeSystemIndexPlugin extends Plugin implements SystemIndexPlugin {
        public static final String SYSTEM_INDEX_NAME = ".test-system-idx";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_NAME + "*", "test"));
        }

        @Override
        public String getFeatureName() {
            return "FakeSystemIndexPlugin";
        }

        @Override
        public String getFeatureDescription() {
            return "FakeSystemIndexPlugin";
        }
    }

    public static class SecondFakeSystemIndexPlugin extends Plugin implements SystemIndexPlugin {
        public static final String SYSTEM_INDEX_NAME = ".another-test-system-idx";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_NAME + "*", "test"));
        }

        @Override
        public String getFeatureName() {
            return "SecondFakeSystemIndexPlugin";
        }

        @Override
        public String getFeatureDescription() {
            return "Fake system index";
        }
    }

    public void testAutoFollow() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        createLeaderIndex("logs-201812", leaderIndexSettings);

        // Enabling auto following:
        if (randomBoolean()) {
            putAutoFollowPatterns("my-pattern", new String[] { "logs-*", "transactions-*" });
        } else {
            putAutoFollowPatterns("my-pattern1", new String[] { "logs-*" });
            putAutoFollowPatterns("my-pattern2", new String[] { "transactions-*" });
        }

        createLeaderIndex("metrics-201901", leaderIndexSettings);

        createLeaderIndex("logs-201901", leaderIndexSettings);
        assertLongBusy(() -> {
            IndicesExistsRequest request = new IndicesExistsRequest("copy-logs-201901");
            assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());
        });
        createLeaderIndex("transactions-201901", leaderIndexSettings);
        assertLongBusy(() -> {
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
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        putAutoFollowPatterns("my-pattern", new String[] { "logs-*" });
        createLeaderIndex("logs-201901", leaderIndexSettings);
        assertLongBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));

            IndicesExistsRequest request = new IndicesExistsRequest("copy-logs-201901");
            assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());

            Metadata metadata = getFollowerCluster().clusterService().state().metadata();
            String leaderIndexUUID = metadata.index("copy-logs-201901")
                .getCustomData(CcrConstants.CCR_CUSTOM_METADATA_KEY)
                .get(CcrConstants.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
            AutoFollowMetadata autoFollowMetadata = metadata.custom(AutoFollowMetadata.TYPE);
            assertThat(autoFollowMetadata, notNullValue());
            List<String> followedLeaderIndixUUIDs = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern");
            assertThat(followedLeaderIndixUUIDs.size(), equalTo(1));
            assertThat(followedLeaderIndixUUIDs.get(0), equalTo(leaderIndexUUID));
        });

        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("logs-201901");
        assertAcked(leaderClient().admin().indices().delete(deleteIndexRequest).actionGet());

        assertLongBusy(() -> {
            AutoFollowMetadata autoFollowMetadata = getFollowerCluster().clusterService()
                .state()
                .metadata()
                .custom(AutoFollowMetadata.TYPE);
            assertThat(autoFollowMetadata, notNullValue());
            List<String> followedLeaderIndixUUIDs = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern");
            assertThat(followedLeaderIndixUUIDs.size(), equalTo(0));
        });
    }

    public void testAutoFollowManyIndices() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        putAutoFollowPatterns("my-pattern", new String[] { "logs-*" });
        long numIndices = randomIntBetween(4, 8);
        for (int i = 0; i < numIndices; i++) {
            createLeaderIndex("logs-" + i, leaderIndexSettings);
        }
        long expectedVal1 = numIndices;
        Metadata[] metadata = new Metadata[1];
        AutoFollowStats[] autoFollowStats = new AutoFollowStats[1];
        try {
            assertLongBusy(() -> {
                metadata[0] = getFollowerCluster().clusterService().state().metadata();
                autoFollowStats[0] = getAutoFollowStats();

                assertThat(metadata[0].indices().size(), equalTo((int) expectedVal1));
                AutoFollowMetadata autoFollowMetadata = metadata[0].custom(AutoFollowMetadata.TYPE);
                assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern"), hasSize((int) expectedVal1));
                assertThat(autoFollowStats[0].getNumberOfSuccessfulFollowIndices(), equalTo(expectedVal1));
            });
        } catch (AssertionError ae) {
            logger.warn("indices={}", Arrays.toString(metadata[0].indices().keys().toArray(String.class)));
            logger.warn("auto follow stats={}", Strings.toString(autoFollowStats[0]));
            throw ae;
        }

        // Delete auto follow pattern and make sure that in the background the auto follower has stopped
        // then the leader index created after that should never be auto followed:
        deleteAutoFollowPattern("my-pattern");
        try {
            assertLongBusy(() -> {
                metadata[0] = getFollowerCluster().clusterService().state().metadata();
                autoFollowStats[0] = getAutoFollowStats();

                assertThat(metadata[0].indices().size(), equalTo((int) expectedVal1));
                AutoFollowMetadata autoFollowMetadata = metadata[0].custom(AutoFollowMetadata.TYPE);
                assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern"), nullValue());
                assertThat(autoFollowStats[0].getAutoFollowedClusters().size(), equalTo(0));
            });
        } catch (AssertionError ae) {
            logger.warn("indices={}", Arrays.toString(metadata[0].indices().keys().toArray(String.class)));
            logger.warn("auto follow stats={}", Strings.toString(autoFollowStats[0]));
            throw ae;
        }
        createLeaderIndex("logs-does-not-count", leaderIndexSettings);

        putAutoFollowPatterns("my-pattern", new String[] { "logs-*" });
        long i = numIndices;
        numIndices = numIndices + randomIntBetween(4, 8);
        for (; i < numIndices; i++) {
            createLeaderIndex("logs-" + i, leaderIndexSettings);
        }
        long expectedVal2 = numIndices;

        assertLongBusy(() -> {
            metadata[0] = getFollowerCluster().clusterService().state().metadata();
            autoFollowStats[0] = getAutoFollowStats();

            assertThat(metadata[0].indices().size(), equalTo((int) expectedVal2));
            AutoFollowMetadata autoFollowMetadata = metadata[0].custom(AutoFollowMetadata.TYPE);
            // expectedVal2 + 1, because logs-does-not-count is also marked as auto followed.
            // (This is because indices created before a pattern exists are not auto followed and are just marked as such.)
            assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern"), hasSize((int) expectedVal2 + 1));
            long count = Arrays.stream(metadata[0].getConcreteAllIndices()).filter(s -> s.startsWith("copy-")).count();
            assertThat(count, equalTo(expectedVal2));
            // Ensure that there are no auto follow errors:
            // (added specifically to see that there are no leader indices auto followed multiple times)
            assertThat(autoFollowStats[0].getRecentAutoFollowErrors().size(), equalTo(0));
        });
    }

    public void testAutoFollowParameterAreDelegated() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
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
        assertLongBusy(() -> {
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
                assertThat(
                    followParameters.getMaxOutstandingReadRequests(),
                    equalTo(request.getParameters().getMaxOutstandingReadRequests())
                );
            }
            if (request.getParameters().getMaxOutstandingWriteRequests() != null) {
                assertThat(
                    followParameters.getMaxOutstandingWriteRequests(),
                    equalTo(request.getParameters().getMaxOutstandingWriteRequests())
                );
            }
            if (request.getParameters().getMaxReadRequestOperationCount() != null) {
                assertThat(
                    followParameters.getMaxReadRequestOperationCount(),
                    equalTo(request.getParameters().getMaxReadRequestOperationCount())
                );
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
                assertThat(
                    followParameters.getMaxWriteRequestOperationCount(),
                    equalTo(request.getParameters().getMaxWriteRequestOperationCount())
                );
            }
            if (request.getParameters().getMaxWriteRequestSize() != null) {
                assertThat(followParameters.getMaxWriteRequestSize(), equalTo(request.getParameters().getMaxWriteRequestSize()));
            }
        });
    }

    public void testConflictingPatterns() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        // Enabling auto following:
        putAutoFollowPatterns("my-pattern1", new String[] { "logs-*" });
        putAutoFollowPatterns("my-pattern2", new String[] { "logs-2018*" });

        createLeaderIndex("logs-201701", leaderIndexSettings);
        assertLongBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));
            assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(0L));
            assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(0L));
        });
        IndicesExistsRequest request = new IndicesExistsRequest("copy-logs-201701");
        assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());

        createLeaderIndex("logs-201801", leaderIndexSettings);
        assertLongBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));
            assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), greaterThanOrEqualTo(1L));
            assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(0L));

            assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(2));
            ElasticsearchException autoFollowError1 = autoFollowStats.getRecentAutoFollowErrors().get("my-pattern1:logs-201801").v2();
            assertThat(autoFollowError1, notNullValue());
            assertThat(
                autoFollowError1.getRootCause().getMessage(),
                equalTo("index to follow [logs-201801] for pattern [my-pattern1] " + "matches with other patterns [my-pattern2]")
            );

            ElasticsearchException autoFollowError2 = autoFollowStats.getRecentAutoFollowErrors().get("my-pattern2:logs-201801").v2();
            assertThat(autoFollowError2, notNullValue());
            assertThat(
                autoFollowError2.getRootCause().getMessage(),
                equalTo("index to follow [logs-201801] for pattern [my-pattern2] " + "matches with other patterns [my-pattern1]")
            );
        });

        request = new IndicesExistsRequest("copy-logs-201801");
        assertFalse(followerClient().admin().indices().exists(request).actionGet().isExists());
    }

    public void testAutoFollowSoftDeletesDisabled() throws Exception {
        putAutoFollowPatterns("my-pattern1", new String[] { "logs-*" });

        // Soft deletes are disabled:
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
        createLeaderIndex("logs-20200101", leaderIndexSettings);
        assertBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
            assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(1L));
            assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(1));
            ElasticsearchException failure = autoFollowStats.getRecentAutoFollowErrors().firstEntry().getValue().v2();
            assertThat(
                failure.getMessage(),
                equalTo("index [logs-20200101] cannot be followed, " + "because soft deletes are not enabled")
            );
            IndicesExistsRequest request = new IndicesExistsRequest("copy-logs-20200101");
            assertFalse(followerClient().admin().indices().exists(request).actionGet().isExists());
        });

        // Soft deletes are enabled:
        leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
        createLeaderIndex("logs-20200102", leaderIndexSettings);
        assertBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));
            IndicesExistsRequest request = new IndicesExistsRequest("copy-logs-20200102");
            assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());
        });
    }

    public void testPauseAndResumeAutoFollowPattern() throws Exception {
        final Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        // index created in the remote cluster before the auto follow pattern exists won't be auto followed
        createLeaderIndex("test-existing-index-is-ignored", leaderIndexSettings);

        // create the auto follow pattern
        putAutoFollowPatterns("test-pattern", new String[] { "test-*", "tests-*" });
        assertLongBusy(() -> {
            final AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getAutoFollowedClusters().size(), equalTo(1));
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
        });

        // index created in the remote cluster are auto followed
        createLeaderIndex("test-new-index-is-auto-followed", leaderIndexSettings);
        assertLongBusy(() -> {
            final AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getAutoFollowedClusters().size(), equalTo(1));
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));
            IndicesExistsRequest request = new IndicesExistsRequest("copy-test-new-index-is-auto-followed");
            assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());
        });
        ensureFollowerGreen("copy-test-new-index-is-auto-followed");

        // pause the auto follow pattern
        pauseAutoFollowPattern("test-pattern");
        assertBusy(() -> assertThat(getAutoFollowStats().getAutoFollowedClusters().size(), equalTo(0)));

        // indices created in the remote cluster are not auto followed because the pattern is paused
        final int nbIndicesCreatedWhilePaused = randomIntBetween(1, 5);
        for (int i = 0; i < nbIndicesCreatedWhilePaused; i++) {
            createLeaderIndex("test-index-created-while-pattern-is-paused-" + i, leaderIndexSettings);
        }

        // sometimes create another index in the remote cluster and close (or delete) it right away
        // it should not be auto followed when the pattern is resumed
        if (randomBoolean()) {
            final String indexName = "test-index-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
            createLeaderIndex(indexName, leaderIndexSettings);
            if (randomBoolean()) {
                assertAcked(leaderClient().admin().indices().prepareClose(indexName));
            } else {
                assertAcked(leaderClient().admin().indices().prepareDelete(indexName));
            }
        }

        if (randomBoolean()) {
            createLeaderIndex("logs-20200101", leaderIndexSettings);
        }

        // pattern is paused, none of the newly created indices has been followed yet
        assertThat(followerClient().admin().indices().prepareStats("copy-*").get().getIndices().size(), equalTo(1));
        ensureLeaderGreen("test-index-created-while-pattern-is-paused-*");

        // resume the auto follow pattern, indices created while the pattern was paused are picked up for auto-following
        resumeAutoFollowPattern("test-pattern");
        assertLongBusy(() -> {
            final Client client = followerClient();
            assertThat(getAutoFollowStats().getAutoFollowedClusters().size(), equalTo(1));
            assertThat(
                client.admin()
                    .cluster()
                    .prepareState()
                    .clear()
                    .setIndices("copy-*")
                    .setMetadata(true)
                    .get()
                    .getState()
                    .getMetadata()
                    .getIndices()
                    .size(),
                equalTo(1 + nbIndicesCreatedWhilePaused)
            );
            for (int i = 0; i < nbIndicesCreatedWhilePaused; i++) {
                IndicesExistsRequest request = new IndicesExistsRequest("copy-test-index-created-while-pattern-is-paused-" + i);
                assertTrue(followerClient().admin().indices().exists(request).actionGet().isExists());
            }
        });
    }

    public void testPauseAndResumeWithMultipleAutoFollowPatterns() throws Exception {
        final Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        final String[] prefixes = { "logs-", "users-", "docs-", "monitoring-", "data-", "system-", "events-", "files-" };

        // create an auto follow pattern for each prefix
        final List<String> autoFollowPatterns = Arrays.stream(prefixes).map(prefix -> {
            final String pattern = prefix + "pattern";
            putAutoFollowPatterns(pattern, new String[] { prefix + "*" });
            return pattern;
        }).collect(Collectors.toList());

        // pick up some random pattern to pause
        final List<String> pausedAutoFollowerPatterns = randomSubsetOf(randomIntBetween(1, 3), autoFollowPatterns);

        // all patterns should be active
        assertBusy(() -> autoFollowPatterns.forEach(pattern -> assertTrue(getAutoFollowPattern(pattern).isActive())));
        assertBusy(() -> assertThat(getAutoFollowStats().getAutoFollowedClusters().size(), equalTo(1)));

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger leaderIndices = new AtomicInteger(0);
        final CountDownLatch latchThree = new CountDownLatch(3);
        final CountDownLatch latchSix = new CountDownLatch(6);
        final CountDownLatch latchNine = new CountDownLatch(9);

        // start creating new indices on the remote cluster
        final Thread createNewLeaderIndicesThread = new Thread(() -> {
            while (running.get() && leaderIndices.get() < 20) {
                final String prefix = randomFrom(prefixes);
                final String leaderIndex = prefix + leaderIndices.incrementAndGet();
                try {
                    createLeaderIndex(leaderIndex, leaderIndexSettings);
                    ensureLeaderGreen(leaderIndex);
                    if (pausedAutoFollowerPatterns.stream().noneMatch(pattern -> pattern.startsWith(prefix))) {
                        ensureFollowerGreen("copy-" + leaderIndex);
                    } else {
                        Thread.sleep(200L);
                    }
                    latchThree.countDown();
                    latchSix.countDown();
                    latchNine.countDown();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
        });
        createNewLeaderIndicesThread.start();

        // wait for 3 leader indices to be created on the remote cluster
        latchThree.await(60L, TimeUnit.SECONDS);
        assertThat(leaderIndices.get(), greaterThanOrEqualTo(3));
        assertLongBusy(() -> assertThat(getAutoFollowStats().getNumberOfSuccessfulFollowIndices(), greaterThanOrEqualTo(3L)));

        // now pause some random patterns
        pausedAutoFollowerPatterns.forEach(this::pauseAutoFollowPattern);
        assertLongBusy(
            () -> autoFollowPatterns.forEach(
                pattern -> assertThat(
                    getAutoFollowPattern(pattern).isActive(),
                    equalTo(pausedAutoFollowerPatterns.contains(pattern) == false)
                )
            )
        );

        // wait for more leader indices to be created on the remote cluster
        latchSix.await(60L, TimeUnit.SECONDS);
        assertThat(leaderIndices.get(), greaterThanOrEqualTo(6));

        // resume auto follow patterns
        pausedAutoFollowerPatterns.forEach(this::resumeAutoFollowPattern);
        assertLongBusy(() -> autoFollowPatterns.forEach(pattern -> assertTrue(getAutoFollowPattern(pattern).isActive())));

        // wait for more leader indices to be created on the remote cluster
        latchNine.await(60L, TimeUnit.SECONDS);
        assertThat(leaderIndices.get(), greaterThanOrEqualTo(9));
        assertLongBusy(() -> assertThat(getAutoFollowStats().getNumberOfSuccessfulFollowIndices(), greaterThanOrEqualTo(9L)));

        running.set(false);
        createNewLeaderIndicesThread.join();

        // check that all leader indices have been correctly auto followed
        List<String> matchingPrefixes = Arrays.stream(prefixes).map(prefix -> prefix + "*").collect(Collectors.toList());
        for (IndexMetadata leaderIndexMetadata : leaderClient().admin().cluster().prepareState().get().getState().metadata()) {
            final String leaderIndex = leaderIndexMetadata.getIndex().getName();
            if (Regex.simpleMatch(matchingPrefixes, leaderIndex)) {
                String followingIndex = "copy-" + leaderIndex;
                assertBusy(
                    () -> assertThat(
                        "Following index [" + followingIndex + "] must exists",
                        followerClient().admin().indices().exists(new IndicesExistsRequest(followingIndex)).actionGet().isExists(),
                        is(true)
                    )
                );
            }
        }

        autoFollowPatterns.forEach(this::deleteAutoFollowPattern);

        ensureFollowerGreen("copy-*");
        assertThat(followerClient().admin().indices().prepareStats("copy-*").get().getIndices().size(), equalTo(leaderIndices.get()));
    }

    public void testAutoFollowExclusion() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
        putAutoFollowPatterns("my-pattern1", new String[] { "logs-*" }, Collections.singletonList("logs-2018*"));

        createLeaderIndex("logs-201801", leaderIndexSettings);
        AutoFollowStats autoFollowStats = getAutoFollowStats();
        assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(0L));
        assertFalse(indexExists("copy-logs-201801", followerClient()));

        createLeaderIndex("logs-201701", leaderIndexSettings);
        assertLongBusy(() -> {
            AutoFollowStats autoFollowStatsResponse = getAutoFollowStats();
            assertThat(autoFollowStatsResponse.getNumberOfSuccessfulFollowIndices(), equalTo(1L));
            assertThat(autoFollowStatsResponse.getNumberOfFailedFollowIndices(), greaterThanOrEqualTo(0L));
            assertThat(autoFollowStatsResponse.getNumberOfFailedRemoteClusterStateRequests(), equalTo(0L));
        });
        assertTrue(indexExists("copy-logs-201701", followerClient()));
        assertFalse(indexExists("copy-logs-201801", followerClient()));
    }

    public void testGetAutoFollowedSystemIndices() throws Exception {
        assertThat(getFollowerAutoFollowedSystemIndices(), is(empty()));

        // This index is created before the auto-follow pattern therefore it won't be auto-followed
        // but it's in the followedLeaderIndexUUIDs list anyway.
        createLeaderSystemIndex(FakeSystemIndexPlugin.SYSTEM_INDEX_NAME);

        putAutoFollowPatterns("my-pattern", new String[] { ".*", "logs-*" });

        assertLongBusy(() -> {
            final AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getAutoFollowedClusters().size(), equalTo(1));
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
        });

        assertThat(getFollowerAutoFollowedSystemIndices(), is(empty()));

        Settings leaderIndexSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
        createLeaderIndex("logs-202101", leaderIndexSettings);
        createLeaderSystemIndex(SecondFakeSystemIndexPlugin.SYSTEM_INDEX_NAME);

        final String followerSystemIndexName = "copy-" + SecondFakeSystemIndexPlugin.SYSTEM_INDEX_NAME;

        ensureFollowerGreen(followerSystemIndexName);
        ensureFollowerGreen("copy-logs-202101");

        assertLongBusy(() -> {
            final AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(2L));

            // Ensure that the operations have been replicated
            final GetResponse response = followerClient().prepareGet(followerSystemIndexName, "_doc", "1").execute().actionGet();
            assertThat(response.isExists(), equalTo(true));
        });

        final List<String> autoFollowedIndices = getFollowerAutoFollowedSystemIndices();
        assertThat(autoFollowedIndices.size(), is(equalTo(1)));
        assertThat(autoFollowedIndices.get(0), is(equalTo(followerSystemIndexName)));

        followerClient().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request(followerSystemIndexName)).actionGet();

        assertLongBusy(() -> { assertThat(getFollowerAutoFollowedSystemIndices(), is(empty())); });
    }

    private void createLeaderSystemIndex(String indexName) {
        leaderClient().index(new IndexRequest(indexName).id("1").source("completed", true)).actionGet();
        final GetResponse getResponse = leaderClient().prepareGet(indexName, "_doc", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
    }

    private List<String> getFollowerAutoFollowedSystemIndices() {
        final ClusterService followerClusterService = getFollowerCluster().getAnyMasterNodeInstance(ClusterService.class);
        PlainActionFuture<List<String>> future = PlainActionFuture.newFuture();
        CcrAutoFollowInfoFetcher.getAutoFollowedSystemIndices(followerClient(), followerClusterService.state(), future);
        return future.actionGet();
    }

    private boolean indexExists(String index, Client client) {
        return client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();
    }

    public void testAutoFollowDatastreamWithClosingFollowerIndex() throws Exception {
        final String datastream = "logs-1";
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("template-id");
        request.indexTemplate(
            new ComposableIndexTemplate(
                org.elasticsearch.core.List.of("logs-*"),
                new Template(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build(),
                    null,
                    null
                ),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
            )
        );
        assertAcked(leaderClient().execute(PutComposableIndexTemplateAction.INSTANCE, request).get());

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(datastream);
        assertAcked(leaderClient().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get());
        leaderClient().prepareIndex(datastream, "_doc")
            .setCreate(true)
            .setSource("foo", "bar", "@timestamp", randomNonNegativeLong())
            .get();

        PutAutoFollowPatternAction.Request followRequest = new PutAutoFollowPatternAction.Request();
        followRequest.setName("pattern-1");
        followRequest.setRemoteCluster("leader_cluster");
        followRequest.setLeaderIndexPatterns(org.elasticsearch.core.List.of("logs-*"));
        followRequest.setFollowIndexNamePattern("{{leader_index}}");
        assertTrue(followerClient().execute(PutAutoFollowPatternAction.INSTANCE, followRequest).get().isAcknowledged());

        logger.info("--> roll over once and wait for the auto-follow to pick up the new index");
        leaderClient().admin().indices().prepareRolloverIndex("logs-1").get();
        assertLongBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));
        });

        ensureFollowerGreen("*");

        final RolloverResponse rolloverResponse = leaderClient().admin().indices().prepareRolloverIndex(datastream).get();
        final String indexInDatastream = rolloverResponse.getOldIndex();

        logger.info("--> closing [{}] on follower so it will be re-opened by crr", indexInDatastream);
        assertAcked(followerClient().admin().indices().prepareClose(indexInDatastream).setMasterNodeTimeout(TimeValue.MAX_VALUE).get());

        logger.info("--> deleting and recreating index [{}] on leader to change index uuid on leader", indexInDatastream);
        assertAcked(leaderClient().admin().indices().prepareDelete(indexInDatastream).get());
        assertAcked(
            leaderClient().admin()
                .indices()
                .prepareCreate(indexInDatastream)
                .addMapping("_doc", MetadataIndexTemplateService.DEFAULT_TIMESTAMP_MAPPING, XContentType.JSON)
                .get()
        );
        leaderClient().prepareIndex(indexInDatastream, "_doc")
            .setCreate(true)
            .setSource("foo", "bar", "@timestamp", randomNonNegativeLong())
            .get();
        leaderClient().execute(
            ModifyDataStreamsAction.INSTANCE,
            new ModifyDataStreamsAction.Request(
                org.elasticsearch.core.List.of(DataStreamAction.addBackingIndex(datastream, indexInDatastream))
            )
        ).get();

        assertLongBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(3L));
        });

        final Metadata metadata = followerClient().admin().cluster().prepareState().get().getState().metadata();
        final DataStream dataStream = metadata.dataStreams().get(datastream);
        assertTrue(dataStream.getIndices().stream().anyMatch(i -> i.getName().equals(indexInDatastream)));
        assertEquals(IndexMetadata.State.OPEN, metadata.index(indexInDatastream).getState());
        ensureFollowerGreen("*");
        final IndicesStatsResponse stats = followerClient().admin().indices().prepareStats(datastream).get();
        assertThat(stats.getIndices(), aMapWithSize(2));

        assertAcked(leaderClient().admin().indices().prepareDelete(indexInDatastream).get());
        assertAcked(followerClient().admin().indices().prepareDelete(indexInDatastream).setMasterNodeTimeout(TimeValue.MAX_VALUE).get());
        ensureFollowerGreen("*");
        final IndicesStatsResponse statsAfterDelete = followerClient().admin().indices().prepareStats(datastream).get();
        assertThat(statsAfterDelete.getIndices(), aMapWithSize(1));
        assertThat(statsAfterDelete.getIndices(), hasKey(rolloverResponse.getNewIndex()));

        assertAcked(leaderClient().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(datastream)).get());
        assertAcked(followerClient().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(datastream)).get());
    }

    private void putAutoFollowPatterns(String name, String[] patterns) {
        putAutoFollowPatterns(name, patterns, Collections.emptyList());
    }

    private void putAutoFollowPatterns(String name, String[] patterns, List<String> exclusionPatterns) {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setName(name);
        request.setRemoteCluster("leader_cluster");
        request.setLeaderIndexPatterns(Arrays.asList(patterns));
        request.setLeaderIndexExclusionPatterns(exclusionPatterns);
        // Need to set this, because following an index in the same cluster
        request.setFollowIndexNamePattern("copy-{{leader_index}}");

        assertTrue(followerClient().execute(PutAutoFollowPatternAction.INSTANCE, request).actionGet().isAcknowledged());
    }

    private void deleteAutoFollowPattern(final String name) {
        DeleteAutoFollowPatternAction.Request request = new DeleteAutoFollowPatternAction.Request(name);
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

    private void pauseAutoFollowPattern(final String name) {
        ActivateAutoFollowPatternAction.Request request = new ActivateAutoFollowPatternAction.Request(name, false);
        assertAcked(followerClient().execute(ActivateAutoFollowPatternAction.INSTANCE, request).actionGet());
    }

    private void resumeAutoFollowPattern(final String name) {
        ActivateAutoFollowPatternAction.Request request = new ActivateAutoFollowPatternAction.Request(name, true);
        assertAcked(followerClient().execute(ActivateAutoFollowPatternAction.INSTANCE, request).actionGet());
    }

    private AutoFollowMetadata.AutoFollowPattern getAutoFollowPattern(final String name) {
        GetAutoFollowPatternAction.Request request = new GetAutoFollowPatternAction.Request();
        request.setName(name);
        GetAutoFollowPatternAction.Response response = followerClient().execute(GetAutoFollowPatternAction.INSTANCE, request).actionGet();
        assertTrue(response.getAutoFollowPatterns().containsKey(name));
        return response.getAutoFollowPatterns().get(name);
    }

    private void assertLongBusy(CheckedRunnable<Exception> codeBlock) throws Exception {
        try {
            assertBusy(codeBlock, 120L, TimeUnit.SECONDS);
        } catch (AssertionError ae) {
            AutoFollowStats autoFollowStats = null;
            try {
                autoFollowStats = getAutoFollowStats();
            } catch (Exception e) {
                ae.addSuppressed(e);
            }
            final AutoFollowStats finalAutoFollowStats = autoFollowStats;
            logger.warn(
                () -> new ParameterizedMessage(
                    "AssertionError when waiting for auto-follower, auto-follow stats are: {}",
                    finalAutoFollowStats != null ? Strings.toString(finalAutoFollowStats) : "null"
                ),
                ae
            );
            throw ae;
        }
    }
}
