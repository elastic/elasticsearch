/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowerInfo;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AutoFollowIT extends CcrIntegTestCase {

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testAutoFollow() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
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
        assertLongBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(2L));
            assertTrue(ESIntegTestCase.indexExists("copy-transactions-201901", followerClient()));
        });

        assertFalse(ESIntegTestCase.indexExists("copy-metrics-201901", followerClient()));
        assertFalse(ESIntegTestCase.indexExists("copy-logs-201812", followerClient()));
    }

    public void testCleanFollowedLeaderIndexUUIDs() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        putAutoFollowPatterns("my-pattern", new String[] {"logs-*"});
        createLeaderIndex("logs-201901", leaderIndexSettings);
        assertLongBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));

            assertTrue(ESIntegTestCase.indexExists("copy-logs-201901", followerClient()));

            Metadata metadata = getFollowerCluster().clusterService().state().metadata();
            String leaderIndexUUID = metadata.index("copy-logs-201901")
                .getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY)
                .get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
            AutoFollowMetadata autoFollowMetadata = metadata.custom(AutoFollowMetadata.TYPE);
            assertThat(autoFollowMetadata, notNullValue());
            List<String> followedLeaderIndixUUIDs = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern");
            assertThat(followedLeaderIndixUUIDs.size(), equalTo(1));
            assertThat(followedLeaderIndixUUIDs.get(0), equalTo(leaderIndexUUID));
        });

        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("logs-201901");
        assertAcked(leaderClient().admin().indices().delete(deleteIndexRequest).actionGet());

        assertLongBusy(() -> {
            AutoFollowMetadata autoFollowMetadata = getFollowerCluster().clusterService().state()
                .metadata()
                .custom(AutoFollowMetadata.TYPE);
            assertThat(autoFollowMetadata, notNullValue());
            List<String> followedLeaderIndixUUIDs = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("my-pattern");
            assertThat(followedLeaderIndixUUIDs.size(), equalTo(0));
        });
    }

    public void testAutoFollowManyIndices() throws Exception {
        Settings leaderIndexSettings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        putAutoFollowPatterns("my-pattern", new String[] {"logs-*"});
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

                assertThat(metadata[0].indices().size(), equalTo((int )expectedVal1));
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

        putAutoFollowPatterns("my-pattern", new String[] {"logs-*"});
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
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        // Enabling auto following:
        putAutoFollowPatterns("my-pattern1", new String[] {"logs-*"});
        putAutoFollowPatterns("my-pattern2", new String[] {"logs-2018*"});

        createLeaderIndex("logs-201701", leaderIndexSettings);
        assertLongBusy(() -> {
            AutoFollowStats autoFollowStats = getAutoFollowStats();
            assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(1L));
            assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(0L));
            assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(0L));
        });
        assertTrue(ESIntegTestCase.indexExists("copy-logs-201701", followerClient()));

        createLeaderIndex("logs-201801", leaderIndexSettings);
        assertLongBusy(() -> {
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

    public void testPauseAndResumeAutoFollowPattern() throws Exception {
        final Settings leaderIndexSettings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        // index created in the remote cluster before the auto follow pattern exists won't be auto followed
        createLeaderIndex("test-existing-index-is-ignored", leaderIndexSettings);

        // create the auto follow pattern
        putAutoFollowPatterns("test-pattern", new String[]{"test-*", "tests-*"});
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
            assertTrue(ESIntegTestCase.indexExists("copy-test-new-index-is-auto-followed", followerClient()));
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
            assertThat(client.admin().cluster().prepareState().clear().setIndices("copy-*").setMetadata(true).get()
                .getState().getMetadata().getIndices().size(), equalTo(1 + nbIndicesCreatedWhilePaused));
            for (int i = 0; i < nbIndicesCreatedWhilePaused; i++) {
                assertTrue(ESIntegTestCase.indexExists("copy-test-index-created-while-pattern-is-paused-" + i, client));
            }
        });
    }

    public void testPauseAndResumeWithMultipleAutoFollowPatterns() throws Exception {
        final Settings leaderIndexSettings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();

        final String[] prefixes = {"logs-", "users-", "docs-", "monitoring-", "data-", "system-", "events-", "files-"};

        // create an auto follow pattern for each prefix
        final List<String> autoFollowPatterns = Arrays.stream(prefixes)
            .map(prefix -> {
                final String pattern = prefix + "pattern";
                putAutoFollowPatterns(pattern, new String[]{prefix + "*"});
                return pattern;
            }).collect(toUnmodifiableList());

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
        assertLongBusy(() -> autoFollowPatterns.forEach(pattern ->
            assertThat(getAutoFollowPattern(pattern).isActive(), equalTo(pausedAutoFollowerPatterns.contains(pattern) == false))));

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
                assertBusy(() -> assertThat("Following index [" + followingIndex + "] must exists",
                    ESIntegTestCase.indexExists(followingIndex, followerClient()), is(true)));
            }
        }

        autoFollowPatterns.forEach(this::deleteAutoFollowPattern);

        ensureFollowerGreen("copy-*");
        assertThat(followerClient().admin().indices().prepareStats("copy-*").get().getIndices().size(), equalTo(leaderIndices.get()));
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
            logger.warn(() -> new ParameterizedMessage("AssertionError when waiting for auto-follower, auto-follow stats are: {}",
                finalAutoFollowStats != null ? Strings.toString(finalAutoFollowStats) : "null"), ae);
            throw ae;
        }
    }
}
