/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertMinRetainedSeqNoAdvanced;
import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertRetentionLeasesAdvanced;
import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertShardsHaveSeqNoDocValues;
import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertShardsSeqNoDocValuesCount;
import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.persistGlobalCheckpointOnPrimaryShards;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class CcrSeqNoPruningIT extends CcrIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(CcrRetentionLeaseIT.RetentionLeaseRenewIntervalSettingPlugin.class))
            .collect(Collectors.toList());
    }

    @Override
    protected Settings followerClusterSettings() {
        return Settings.builder()
            .put(super.followerClusterSettings())
            .put(CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(100))
            .build();
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testSeqNoPrunedOnLeaderAfterFollowerCatchesUp() throws Exception {
        final var leaderIndex = randomIdentifier();
        final var followerIndex = "follower-" + leaderIndex;
        final int numberOfShards = 1;

        final var additionalSettings = Map.of(
            IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(),
            "true",
            IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(),
            SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY.toString(),
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueMillis(100).getStringRep()
        );

        final String leaderIndexSettings = getIndexSettings(numberOfShards, 0, additionalSettings);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderGreen(leaderIndex);

        final int nbBatches = randomIntBetween(5, 10);
        final int docsPerBatch = randomIntBetween(20, 50);

        for (int batch = 0; batch < nbBatches; batch++) {
            var bulk = leaderClient().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch; doc++) {
                bulk.add(leaderClient().prepareIndex(leaderIndex).setSource("f", batch * docsPerBatch + doc));
            }
            assertNoFailures(bulk.get());
        }

        flush(leaderClient(), leaderIndex);

        assertShardsHaveSeqNoDocValues(getLeaderCluster(), leaderIndex, true, numberOfShards);
        assertThat(
            leaderClient().admin()
                .indices()
                .prepareStats(leaderIndex)
                .clear()
                .setSegments(true)
                .get()
                .getPrimaries()
                .getSegments()
                .getCount(),
            greaterThan(1L)
        );

        followerClient().execute(PutFollowAction.INSTANCE, putFollow(leaderIndex, followerIndex)).get();

        final long maxSeqNo = getMaxSeqNo(leaderClient(), leaderIndex);
        assertRetentionLeasesAdvanced(leaderClient(), leaderIndex, maxSeqNo + 1);
        persistGlobalCheckpointOnPrimaryShards(getLeaderCluster(), leaderIndex);
        assertMinRetainedSeqNoAdvanced(getLeaderCluster(), leaderIndex, maxSeqNo + 1);

        var forceMerge = leaderClient().admin().indices().prepareForceMerge(leaderIndex).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));
        assertThat(
            leaderClient().admin()
                .indices()
                .prepareStats(leaderIndex)
                .clear()
                .setSegments(true)
                .get()
                .getPrimaries()
                .getSegments()
                .getCount(),
            equalTo(1L)
        );

        refresh(leaderClient(), leaderIndex);
        assertShardsHaveSeqNoDocValues(getLeaderCluster(), leaderIndex, false, numberOfShards);

        ensureFollowerGreen(true, followerIndex);
        assertIndexFullyReplicatedToFollower(leaderIndex, followerIndex);
    }

    public void testSeqNoPartiallyRetainedByCcrLease() throws Exception {
        final var leaderIndex = randomIdentifier();
        final var followerIndex = "follower-" + leaderIndex;
        final int numberOfShards = 1;

        final var additionalSettings = Map.of(
            IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(),
            "true",
            IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(),
            SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY.toString(),
            IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(),
            TimeValue.timeValueMillis(100).getStringRep()
        );

        final String leaderIndexSettings = getIndexSettings(numberOfShards, 0, additionalSettings);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderGreen(leaderIndex);

        followerClient().execute(PutFollowAction.INSTANCE, putFollow(leaderIndex, followerIndex)).get();
        ensureFollowerGreen(true, followerIndex);

        final int nbBatches = randomIntBetween(5, 10);
        final int docsPerBatch = randomIntBetween(20, 50);

        for (int batch = 0; batch < nbBatches; batch++) {
            var bulk = leaderClient().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch; doc++) {
                bulk.add(leaderClient().prepareIndex(leaderIndex).setSource("f", batch * docsPerBatch + doc));
            }
            assertNoFailures(bulk.get());
        }
        flush(leaderClient(), leaderIndex);

        assertIndexFullyReplicatedToFollower(leaderIndex, followerIndex);

        final long leaseSeqNoBeforePause = getMaxSeqNo(leaderClient(), leaderIndex) + 1;
        assertRetentionLeasesAdvanced(leaderClient(), leaderIndex, leaseSeqNoBeforePause);
        pauseFollow(followerIndex);

        final int nbBatches2 = randomIntBetween(5, 10);
        final int docsPerBatch2 = randomIntBetween(20, 50);

        for (int batch = 0; batch < nbBatches2; batch++) {
            var bulk = leaderClient().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch2; doc++) {
                bulk.add(leaderClient().prepareIndex(leaderIndex).setSource("f", batch * docsPerBatch2 + doc));
            }
            assertNoFailures(bulk.get());
        }
        flush(leaderClient(), leaderIndex);

        final long newMaxSeqNo = getMaxSeqNo(leaderClient(), leaderIndex);
        assertBusy(() -> {
            var stats = leaderClient().admin().indices().stats(new IndicesStatsRequest().clear().indices(leaderIndex)).actionGet();
            for (ShardStats shardStats : stats.getShards()) {
                var ccrLeases = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(
                    shardStats.getRetentionLeaseStats().retentionLeases()
                );
                assertThat(ccrLeases.values(), hasSize(1));
                RetentionLease ccrLease = ccrLeases.values().iterator().next();
                assertThat(ccrLease.retainingSequenceNumber(), equalTo(leaseSeqNoBeforePause));

                for (RetentionLease lease : shardStats.getRetentionLeaseStats().retentionLeases().leases()) {
                    if (lease.id().equals(ccrLease.id()) == false) {
                        assertThat(
                            "peer recovery lease [" + lease.id() + "] should have advanced",
                            lease.retainingSequenceNumber(),
                            equalTo(newMaxSeqNo + 1)
                        );
                    }
                }
            }
        });

        var forceMerge = leaderClient().admin().indices().prepareForceMerge(leaderIndex).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));
        flush(leaderClient(), leaderIndex);

        assertShardsHaveSeqNoDocValues(getLeaderCluster(), leaderIndex, true, numberOfShards);

        final long expectedRetainedDocs = newMaxSeqNo + 1 - leaseSeqNoBeforePause;
        assertShardsSeqNoDocValuesCount(getLeaderCluster(), leaderIndex, expectedRetainedDocs, numberOfShards);

        followerClient().execute(ResumeFollowAction.INSTANCE, resumeFollow(followerIndex)).actionGet();
        assertIndexFullyReplicatedToFollower(leaderIndex, followerIndex);

        assertRetentionLeasesAdvanced(leaderClient(), leaderIndex, newMaxSeqNo + 1);

        final int nbBatches3 = randomIntBetween(5, 10);
        final int docsPerBatch3 = randomIntBetween(20, 50);

        for (int batch = 0; batch < nbBatches3; batch++) {
            var bulk = leaderClient().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch3; doc++) {
                bulk.add(leaderClient().prepareIndex(leaderIndex).setSource("f", batch * docsPerBatch3 + doc));
            }
            assertNoFailures(bulk.get());
        }
        flush(leaderClient(), leaderIndex);

        final long finalMaxSeqNo = getMaxSeqNo(leaderClient(), leaderIndex);

        assertRetentionLeasesAdvanced(leaderClient(), leaderIndex, finalMaxSeqNo + 1);
        persistGlobalCheckpointOnPrimaryShards(getLeaderCluster(), leaderIndex);
        flush(leaderClient(), leaderIndex);

        forceMerge = leaderClient().admin().indices().prepareForceMerge(leaderIndex).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));
        refresh(leaderClient(), leaderIndex);

        assertIndexFullyReplicatedToFollower(leaderIndex, followerIndex);

        assertShardsHaveSeqNoDocValues(getLeaderCluster(), leaderIndex, false, numberOfShards);

        final var newFollowerIndex = "new-follower-" + leaderIndex;
        followerClient().execute(PutFollowAction.INSTANCE, putFollow(leaderIndex, newFollowerIndex)).get();
        ensureFollowerGreen(true, newFollowerIndex);

        assertIndexFullyReplicatedToFollower(leaderIndex, newFollowerIndex);
    }

    private static long getMaxSeqNo(Client client, String index) {
        return client.admin().indices().prepareStats(index).get().getShards()[0].getSeqNoStats().getMaxSeqNo();
    }
}
