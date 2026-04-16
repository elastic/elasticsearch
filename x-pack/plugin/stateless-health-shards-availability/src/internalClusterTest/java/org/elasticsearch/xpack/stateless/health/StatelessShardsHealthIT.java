/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.health;

import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;

public class StatelessShardsHealthIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new HashSet<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(StatelessShardsHealthPlugin.class);
        return plugins;
    }

    public void testReplicaShardsAvailabilityOnStateless() throws Exception {
        startMasterAndIndexNode();
        updateClusterSettings(
            Settings.builder()
                .put("health.shards_availability.replica_unassigned_buffer_time", "0s")
                .put("health.shards_availability.primary_unassigned_buffer_time", "0s")
        );

        String indexName = "myindex";
        createIndex(indexName, 2, 0);
        ensureGreen(indexName);

        waitForStatusAndGet(HealthStatus.GREEN);

        // Increase the number of replicas to 2, so that the index goes red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.number_of_replicas", 2))
            .get();

        HealthIndicatorResult hir = waitForStatusAndGet(HealthStatus.RED);
        assertUnavailablePrimaryAndReplicaIndices(hir, null, indexName);
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString("Not all data is searchable. No searchable copies of the data exist on 1 index [myindex].")
        );
        assertThat(hir.symptom(), containsString("This cluster has 4 unavailable replica shards."));
        assertThat(hir.diagnosisList().stream().map(d -> d.definition().id()).toList(), equalTo(List.of("debug_node:role:search")));
        assertThat(
            hir.diagnosisList()
                .stream()
                .flatMap(
                    d -> d.affectedResources()
                        .stream()
                        .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                        .flatMap(r -> r.getValues().stream())
                )
                .toList(),
            equalTo(List.of(indexName))
        );

        // Start a search node so that one of the replicas can be allocated
        startSearchNode();

        hir = waitForStatusAndGet(HealthStatus.YELLOW);
        assertUnavailablePrimaryAndReplicaIndices(hir, null, indexName);
        // We should inherit the existing impacts from the Stateful code version
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString("Searches might be slower than usual. Fewer redundant copies of the data exist on 1 index [myindex].")
        );
        assertThat(hir.symptom(), containsString("This cluster has 2 unavailable replica shards."));
        assertThat(hir.diagnosisList().stream().map(d -> d.definition().id()).toList(), equalTo(List.of("update_shards:role:search")));
        assertThat(
            hir.diagnosisList()
                .stream()
                .flatMap(
                    d -> d.affectedResources()
                        .stream()
                        .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                        .flatMap(r -> r.getValues().stream())
                )
                .toList(),
            equalTo(List.of(indexName))
        );

        // Start one more search node so everything can go green
        startSearchNode();
        waitForStatusAndGet(HealthStatus.GREEN);
    }

    public void testReplicaShardsForNewPrimariesAvailabilityOnStateless() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();
        updateClusterSettings(
            Settings.builder()
                .put("health.shards_availability.replica_unassigned_buffer_time", "0s")
                .put("health.shards_availability.primary_unassigned_buffer_time", "0s")
        );

        // We need to force there to be at least one truly unassigned replica. We create an index with two
        // replicas (there is only one search node) for this purpose.
        createIndex("other", 1, 2);

        // We hook in to the transport service and catch the "shard started" event for the
        // soon-to-be-created "myindex" index. We want to delay it so the primary stays in the
        // "INITIALIZING" state until we release it.
        var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        CountDownLatch latch = new CountDownLatch(1);
        masterTransportService.<ShardStateAction.StartedShardEntry>addRequestHandlingBehavior(
            ShardStateAction.SHARD_STARTED_ACTION_NAME,
            (handler, request, channel, task) -> {
                if (request.toString().contains("[myindex]")) {
                    latch.await(30, TimeUnit.SECONDS);
                }
                handler.messageReceived(request, channel, task);
            }
        );

        // Create the index (don't bother waiting for active shards because we've delayed them until we release the latch)
        String indexName = "myindex";
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1))
            .setWaitForActiveShards(0)
            .get();

        // Health is YELLOW because "other" has a truly unassigned replica (1 search node, 2 replicas).
        // "myindex" primary and replica are both still INITIALIZING (isNew path), so they do not
        // contribute to the YELLOW status.
        assertBusy(() -> {
            GetHealthAction.Response health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 10)).get();
            HealthIndicatorResult hir = health.findIndicator("shards_availability");
            assertThat(hir.status(), equalTo(HealthStatus.YELLOW));
            assertUnavailablePrimaryAndReplicaIndices(hir, null, "other");
            // verify that "myindex" is affected, since issues with "other" can also cause YELLOW health
            assertTrue(hir.diagnosisList().stream().anyMatch(d -> d.affectedResources().get(0).getValues().contains(indexName)));
        });

        // Release the latch so the primary can move from INITIALIZING -> STARTED
        latch.countDown();

        // Delete "other" and make sure cluster goes green
        assertAcked(indicesAdmin().prepareDelete("other"));
        assertBusy(() -> {
            var health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 10)).get();
            assertEquals(HealthStatus.GREEN, health.getStatus());
        });
    }

    public void testReplicaShardsForNewPrimariesAvailabilityWithTimeBuffer() throws Exception {
        runTestReplicaShardsForNewPrimariesAvailability(true);
    }

    public void testReplicaShardsForNewPrimariesAvailabilityNoTimeBufferOrExpired() throws Exception {
        runTestReplicaShardsForNewPrimariesAvailability(false);
    }

    void runTestReplicaShardsForNewPrimariesAvailability(boolean useTimeBuffer) throws Exception {
        // This test checks replica shard availability shortly after the primary has started. It tests with
        // `health.shards_availability.replica_unassigned_buffer_time` both non-zero and zero.
        // When non-zero, GREEN is expected (provisionally unavailable replicas). When zero, RED.

        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        // Ideally, we'd use the default values of 3s when `useTimeBuffer`,
        // but to avoid transient test failures when run is slow, we use 10s.
        var timeBuffer = useTimeBuffer ? "10s" : "0s";
        updateClusterSettings(Settings.builder().put("health.shards_availability.replica_unassigned_buffer_time", timeBuffer));

        // Shard health treats all replicas allocated as an "everything is green" sort of situation.
        // However, replicas that are initializing or within the provisionally-unassigned grace period
        // are ignored when evaluating availability. We need to force there to be at least one
        // truly unassigned replica. We create an index with two replicas (there is only one search node)
        // for this purpose.
        createIndex("other", 1, 2);

        // We hook in to the transport service and catch the "shard started" event for the
        // soon-to-be-created "myindex" index. We want to delay it so the primary stays in the
        // "INITIALIZING" state until we release it.
        var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        masterTransportService.<ShardStateAction.StartedShardEntry>addRequestHandlingBehavior(
            ShardStateAction.SHARD_STARTED_ACTION_NAME,
            (handler, request, channel, task) -> {
                // Wait until after peer recovery so that primary is started, but replica is not
                if (request.toString().contains("[myindex]") && request.toString().contains("[after peer recovery]")) {
                    // release latch1 allowing assertions to run
                    latch1.countDown();
                    // wait until assertions complete
                    latch2.await(30, TimeUnit.SECONDS);
                }
                handler.messageReceived(request, channel, task);
            }
        );

        // Create the index (don't bother waiting for active shards because we've delayed them until we release the latch)
        String indexName = "myindex";
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1))
            .setWaitForActiveShards(0)
            .get();

        // Wait for latch1 so that primary has moved from INITIALIZED to STARTED
        latch1.await(30, TimeUnit.SECONDS);

        // With a grace buffer, shards_availability stays GREEN while replica issues are still provisional.
        // With buffer disabled, the same routing state yields RED.
        assertBusy(() -> {
            GetHealthAction.Response health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 10)).get();
            HealthIndicatorResult hir = health.findIndicator("shards_availability");

            // At this point the primary is STARTED but the replica is still INITIALIZING.
            // If using a time buffer, this should run while still within the buffer time, allowing health to be GREEN
            // If not using a time buffer, this state should cause shard_availability to go RED
            if (useTimeBuffer) {
                assertThat(hir.status(), equalTo(HealthStatus.GREEN));
                // "myindex" and "other" replica are within the grace period. They should appear as provisionally
                // unavailable, not truly unavailable.
                assertProvisionallyUnavailablePrimaryAndReplicaIndices(hir, null, indexName + ", other");
                assertUnavailablePrimaryAndReplicaIndices(hir, null, null);
            } else {
                assertThat(hir.status(), equalTo(HealthStatus.RED));
                assertProvisionallyUnavailablePrimaryAndReplicaIndices(hir, null, null);
                assertUnavailablePrimaryAndReplicaIndices(hir, null, indexName + ", other");
            }

            // verify that "myindex" appears in diagnoses while its shards are still catching up
            assertTrue(hir.diagnosisList().stream().anyMatch(d -> d.affectedResources().get(0).getValues().contains(indexName)));
        }, 30, TimeUnit.SECONDS);

        // Release the latch so the replica can move to STARTED
        latch2.countDown();

        // Delete "other" and make sure cluster goes green
        assertAcked(indicesAdmin().prepareDelete("other"));
        assertBusy(() -> {
            var health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 10)).get();
            assertEquals(HealthStatus.GREEN, health.getStatus());
        });
    }

    public void testIncreaseClusterShardLimit() throws Exception {
        startMasterAndIndexNode();
        String indexNode = startIndexNode();
        startSearchNode();
        String searchNode = startSearchNode();
        updateClusterSettings(
            Settings.builder()
                .put("health.shards_availability.replica_unassigned_buffer_time", "0s")
                .put("health.shards_availability.primary_unassigned_buffer_time", "0s")
        );

        // With this variable we choose if we want this experiment to affect the index or the search nodes
        boolean shutDownIndexNode = randomBoolean();

        client().admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().put(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1))
            .get();

        String indexName = "myindex";
        createIndex(indexName, indexSettings(2, 1).put("index.unassigned.node_left.delayed_timeout", "0ms").build());
        waitForStatusAndGet(HealthStatus.GREEN);

        // Stop a node
        internalCluster().stopNode(shutDownIndexNode ? indexNode : searchNode);

        HealthIndicatorResult hir = waitForStatusAndGet(HealthStatus.RED);
        assertUnavailablePrimaryAndReplicaIndices(hir, shutDownIndexNode ? indexName : null, indexName);
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString(
                shutDownIndexNode
                    ? "Cannot add data to 1 index [myindex]. Searches might return incomplete results."
                    : "Not all data is searchable. No searchable copies of the data exist on 1 index [myindex]."
            )
        );
        assertThat(
            hir.symptom(),
            containsString(
                shutDownIndexNode
                    ? "This cluster has 1 unavailable primary shard, 1 unavailable replica shard."
                    : "This cluster has 1 unavailable replica shard."
            )
        );
        assertThat(hir.diagnosisList().size(), equalTo(shutDownIndexNode ? 2 : 1));
        Diagnosis diagnosis = hir.diagnosisList().get(0);
        assertThat(
            diagnosis.definition().id(),
            equalTo("increase_shard_limit_cluster_setting:role:" + (shutDownIndexNode ? "index" : "search"))
        );
        assertThat(
            diagnosis.affectedResources()
                .stream()
                .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                .flatMap(r -> r.getValues().stream())
                .toList(),
            equalTo(List.of(indexName))
        );

        if (shutDownIndexNode) {
            diagnosis = hir.diagnosisList().get(1);
            assertThat(diagnosis.definition().id(), equalTo("explain_allocations"));
            assertThat(
                diagnosis.affectedResources()
                    .stream()
                    .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                    .flatMap(r -> r.getValues().stream())
                    .toList(),
                equalTo(List.of(indexName))
            );
        }
    }

    public void testIncreaseIndexShardLimit() throws Exception {
        startMasterAndIndexNode();
        String indexNode = startIndexNode();
        startSearchNode();
        String searchNode = startSearchNode();
        updateClusterSettings(
            Settings.builder()
                .put("health.shards_availability.replica_unassigned_buffer_time", "0s")
                .put("health.shards_availability.primary_unassigned_buffer_time", "0s")
        );

        // With this variable we choose if we want this experiment to affect the index or the search nodes
        boolean shutDownIndexNode = randomBoolean();

        String indexName = "myindex";
        createIndex(
            indexName,
            indexSettings(2, 1).put(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
                .put("index.unassigned.node_left.delayed_timeout", "0ms")
                .build()
        );
        waitForStatusAndGet(HealthStatus.GREEN);

        // Stop a node
        internalCluster().stopNode(shutDownIndexNode ? indexNode : searchNode);

        HealthIndicatorResult hir = waitForStatusAndGet(HealthStatus.RED);
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString(
                shutDownIndexNode
                    ? "Cannot add data to 1 index [myindex]. Searches might return incomplete results."
                    : "Not all data is searchable. No searchable copies of the data exist on 1 index [myindex]."
            )
        );
        assertUnavailablePrimaryAndReplicaIndices(hir, shutDownIndexNode ? indexName : null, indexName);
        assertThat(
            hir.symptom(),
            containsString(
                shutDownIndexNode
                    ? "This cluster has 1 unavailable primary shard, 1 unavailable replica shard."
                    : "This cluster has 1 unavailable replica shard."
            )
        );
        assertThat(hir.diagnosisList().size(), equalTo(shutDownIndexNode ? 2 : 1));
        Diagnosis diagnosis = hir.diagnosisList().get(0);
        assertThat(
            diagnosis.definition().id(),
            equalTo("increase_shard_limit_index_setting:role:" + (shutDownIndexNode ? "index" : "search"))
        );
        assertThat(
            diagnosis.affectedResources()
                .stream()
                .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                .flatMap(r -> r.getValues().stream())
                .toList(),
            equalTo(List.of(indexName))
        );

        if (shutDownIndexNode) {
            diagnosis = hir.diagnosisList().get(1);
            assertThat(diagnosis.definition().id(), equalTo("explain_allocations"));
            assertThat(
                diagnosis.affectedResources()
                    .stream()
                    .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                    .flatMap(r -> r.getValues().stream())
                    .toList(),
                equalTo(List.of(indexName))
            );
        }
    }

    public void testAddIndexNodes() throws Exception {
        startMasterOnlyNode();
        String indexNode = startIndexNode();
        updateClusterSettings(
            Settings.builder()
                .put("health.shards_availability.replica_unassigned_buffer_time", "0s")
                .put("health.shards_availability.primary_unassigned_buffer_time", "0s")
        );

        final List<String> indexNames = IntStream.range(0, between(1, 11)).mapToObj(i -> Strings.format("myindex-%02d", i)).toList();
        indexNames.forEach(indexName -> {
            createIndex(indexName, 1, 0);
            ensureGreen(indexName);
        });

        GetHealthAction.Response health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(randomBoolean(), 10))
            .get();
        HealthIndicatorResult hir = health.findIndicator("shards_availability");
        assertThat(hir.status(), equalTo(HealthStatus.GREEN));

        // Stop a node
        internalCluster().stopNode(indexNode);

        hir = waitForStatusAndGet(HealthStatus.RED);

        final String reportedIndices = indexNames.size() > 10
            ? Strings.collectionToDelimitedString(indexNames.subList(0, 10), ", ") + ", ..."
            : Strings.collectionToDelimitedString(indexNames, ", ");

        assertUnavailablePrimaryAndReplicaIndices(hir, reportedIndices, null);

        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString(
                "Cannot add data to "
                    + indexNames.size()
                    + " "
                    + (indexNames.size() == 1 ? "index" : "indices")
                    + " ["
                    + reportedIndices
                    + "]. Searches might return incomplete results."
            )
        );
        assertThat(
            hir.symptom(),
            containsString(
                "This cluster has " + indexNames.size() + " unavailable primary " + (indexNames.size() == 1 ? "shard" : "shards") + "."
            )
        );
        assertThat(hir.diagnosisList().size(), equalTo(1));
        Diagnosis diagnosis = hir.diagnosisList().get(0);
        assertThat(diagnosis.definition().id(), equalTo("debug_node:role:index"));
        final List<String> affectedIndices = diagnosis.affectedResources()
            .stream()
            .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
            .flatMap(r -> r.getValues().stream())
            .toList();
        // Due to the maxAffectedResources cap, we cannot guarantee the order of the affected indices.
        assertThat(affectedIndices, hasSize(Math.min(10, indexNames.size())));
        assertThat(affectedIndices, everyItem(in(indexNames)));

        safeGet(indicesAdmin().prepareDelete(indexNames.toArray(String[]::new)).execute());
    }

    /**
     * Wait for the health report API to report a certain high-level color and return the response
     */
    private HealthIndicatorResult waitForStatusAndGet(HealthStatus color) throws Exception {
        String indicator = "shards_availability";
        HealthIndicatorResult[] result = new HealthIndicatorResult[1];
        assertBusy(() -> {
            logger.info("--> waiting for cluster to be {}...", color);
            GetHealthAction.Request request = new GetHealthAction.Request(indicator, true, 10);
            var health = client().execute(GetHealthAction.INSTANCE, request).get();
            assertThat(health.findIndicator(indicator).status(), equalTo(color));
            result[0] = health.findIndicator(indicator);
        });
        return result[0];
    }

    private void assertUnavailablePrimaryAndReplicaIndices(
        HealthIndicatorResult hir,
        @Nullable String unavailablePrimaries,
        @Nullable String unavailableReplicas
    ) {
        final var details = ((SimpleHealthIndicatorDetails) hir.details()).details();
        assertThat(details.get("indices_with_unavailable_primaries"), equalTo(unavailablePrimaries));
        assertThat(details.get("indices_with_unavailable_replicas"), equalTo(unavailableReplicas));
    }

    private void assertProvisionallyUnavailablePrimaryAndReplicaIndices(
        HealthIndicatorResult hir,
        @Nullable String provisionallyUnavailablePrimaries,
        @Nullable String provisionallyUnavailableReplicas
    ) {
        final var details = ((SimpleHealthIndicatorDetails) hir.details()).details();
        assertThat(details.get("indices_with_provisionally_unavailable_primaries"), equalTo(provisionallyUnavailablePrimaries));
        assertThat(details.get("indices_with_provisionally_unavailable_replicas"), equalTo(provisionallyUnavailableReplicas));
    }
}
