/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.seqno;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertShardsHaveSeqNoDocValues;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class SeqNoPruningIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    public void testSeqNoPrunedAfterMerge() throws Exception {
        assumeTrue("requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
                .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .build()
        );
        ensureGreen(indexName);

        final int nbBatches = randomIntBetween(5, 10);
        final int docsPerBatch = randomIntBetween(20, 50);
        final long totalDocs = (long) nbBatches * docsPerBatch;

        for (int batch = 0; batch < nbBatches; batch++) {
            var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch; doc++) {
                bulk.add(prepareIndex(indexName).setSource("field", "value-" + batch + "-" + doc));
            }
            assertNoFailures(bulk.get());
        }

        flushAndRefresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, true, 1);

        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            greaterThan(1L)
        );

        // waits for retention leases to advance past all docs
        assertBusy(() -> {
            for (var indicesServices : internalCluster().getDataNodeInstances(IndicesService.class)) {
                for (var indexService : indicesServices) {
                    if (indexService.index().getName().equals(indexName)) {
                        for (var indexShard : indexService) {
                            for (RetentionLease lease : indexShard.getRetentionLeases().leases()) {
                                assertThat(
                                    "retention lease [" + lease.id() + "] should have advanced",
                                    lease.retainingSequenceNumber(),
                                    equalTo(totalDocs)
                                );
                            }
                        }
                    }
                }
            }
        });

        var forceMerge = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));

        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            equalTo(1L)
        );

        refresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, false, 1);

        final boolean peerRecovery = randomBoolean();
        if (peerRecovery) {
            logger.info("--> triggering peer recovery by adding a replica");
            setReplicaCount(1, indexName);
            ensureGreen(indexName);
        } else {
            logger.info("--> triggering relocation via move allocation command");
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            var primaryShard = state.routingTable().index(indexName).shard(0).primaryShard();
            String sourceNode = primaryShard.currentNodeId();
            String sourceNodeName = state.nodes().get(sourceNode).getName();
            String targetNodeName = state.nodes()
                .getDataNodes()
                .values()
                .stream()
                .filter(n -> n.getName().equals(sourceNodeName) == false)
                .findFirst()
                .orElseThrow()
                .getName();

            ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceNodeName, targetNodeName));
            waitForRelocation();
        }

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, false, peerRecovery ? 2 : 1);
    }
}
