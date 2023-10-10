/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.recovery;

import org.apache.lucene.tests.util.English;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFileChunkRequest;
import org.elasticsearch.node.RecoverySettingsChunkSizePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.node.RecoverySettingsChunkSizePlugin.CHUNK_SIZE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
@SuppressCodecs("*") // test relies on exact file extensions
public class TruncatedRecoveryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, RecoverySettingsChunkSizePlugin.class);
    }

    /**
     * This test tries to truncate some of larger files in the index to trigger leftovers on the recovery
     * target. This happens during recovery when the last chunk of the file is transferred to the replica
     * we just throw an exception to make sure the recovery fails and we leave some half baked files on the target.
     * Later we allow full recovery to ensure we can still recover and don't run into corruptions.
     */
    public void testCancelRecoveryAndResume() throws Exception {
        updateClusterSettings(
            Settings.builder().put(CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(randomIntBetween(50, 300), ByteSizeUnit.BYTES))
        );

        NodesStatsResponse nodeStats = clusterAdmin().prepareNodesStats().get();
        List<NodeStats> dataNodeStats = new ArrayList<>();
        for (NodeStats stat : nodeStats.getNodes()) {
            if (stat.getNode().canContainData()) {
                dataNodeStats.add(stat);
            }
        }
        assertThat(dataNodeStats.size(), greaterThanOrEqualTo(2));
        Collections.shuffle(dataNodeStats, random());
        // we use 2 nodes a lucky and unlucky one
        // the lucky one holds the primary
        // the unlucky one gets the replica and the truncated leftovers
        NodeStats primariesNode = dataNodeStats.get(0);
        NodeStats unluckyNode = dataNodeStats.get(1);

        // create the index and prevent allocation on any other nodes than the lucky one
        // we have no replicas so far and make sure that we allocate the primary on the lucky node
        assertAcked(
            prepareCreate("test").setMapping("field1", "type=text", "the_id", "type=text")
                .setSettings(
                    indexSettings(numberOfShards(), 0).put("index.routing.allocation.include._name", primariesNode.getNode().getName())
                )
        ); // only allocate on the lucky node

        // index some docs and check if they are coming back
        int numDocs = randomIntBetween(100, 200);
        List<IndexRequestBuilder> builder = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            builder.add(client().prepareIndex("test").setId(id).setSource("field1", English.intToEnglish(i), "the_id", id));
        }
        indexRandom(true, builder);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            assertHitCount(client().prepareSearch().setQuery(QueryBuilders.termQuery("the_id", id)).get(), 1);
        }
        ensureGreen();
        // ensure we have flushed segments and make them a big one via optimize
        indicesAdmin().prepareFlush().setForce(true).get();
        indicesAdmin().prepareFlush().setForce(true).get(); // double flush to create safe commit in case of async durability
        indicesAdmin().prepareForceMerge().setMaxNumSegments(1).setFlush(true).get();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean truncate = new AtomicBoolean(true);
        for (NodeStats dataNode : dataNodeStats) {
            MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(
                TransportService.class,
                dataNode.getNode().getName()
            ));
            mockTransportService.addSendBehavior(
                internalCluster().getInstance(TransportService.class, unluckyNode.getNode().getName()),
                (connection, requestId, action, request, options) -> {
                    if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                        RecoveryFileChunkRequest req = (RecoveryFileChunkRequest) request;
                        logger.info("file chunk [{}] lastChunk: {}", req, req.lastChunk());
                        if ((req.name().endsWith("cfs") || req.name().endsWith("fdt")) && req.lastChunk() && truncate.get()) {
                            latch.countDown();
                            throw new RuntimeException("Caused some truncated files for fun and profit");
                        }
                    }
                    connection.sendRequest(requestId, action, request, options);
                }
            );
        }

        logger.info("--> bumping replicas to 1"); //
        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(
                    "index.routing.allocation.include._name",  // now allow allocation on all nodes
                    primariesNode.getNode().getName() + "," + unluckyNode.getNode().getName()
                ),
            "test"
        );

        latch.await();

        // at this point we got some truncated left overs on the replica on the unlucky node
        // now we are allowing the recovery to allocate again and finish to see if we wipe the truncated files
        truncate.compareAndSet(true, false);
        ensureGreen("test");
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            assertHitCount(client().prepareSearch().setQuery(QueryBuilders.termQuery("the_id", id)).get(), 1);
        }
    }
}
