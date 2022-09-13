/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.NodeDecision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.greaterThan;

public class ReactiveReasonTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testXContent() throws IOException {
        String reason = randomAlphaOfLength(10);
        long unassigned = randomNonNegativeLong();
        long assigned = randomNonNegativeLong();
        String indexUUID = UUIDs.randomBase64UUID();
        String indexName = randomAlphaOfLength(10);
        SortedSet<ShardId> unassignedShardIds = new TreeSet<>(randomUnique(() -> new ShardId(indexName, indexUUID, randomInt(1000)), 600));
        SortedSet<ShardId> assignedShardIds = new TreeSet<>(randomUnique(() -> new ShardId(indexName, indexUUID, randomInt(1000)), 600));
        DiscoveryNode discoveryNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        List<NodeDecision> unassignedShardAllocationDecision = List.of(
            new NodeDecision(discoveryNode, Decision.single(Decision.Type.NO, "no_label", "No space to allocate"))
        );
        List<NodeDecision> assignedShardAllocateDecision = List.of(
            new NodeDecision(discoveryNode, Decision.single(Decision.Type.YES, "yes_label", "There's enough space"))
        );
        var reactiveReason = new ReactiveStorageDeciderService.ReactiveReason(
            reason,
            unassigned,
            unassignedShardIds,
            assigned,
            assignedShardIds,
            unassignedShardAllocationDecision,
            assignedShardAllocateDecision
        );

        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                BytesReference.bytes(reactiveReason.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            )
        ) {
            Map<String, Object> map = parser.map();
            assertEquals(reason, map.get("reason"));
            assertEquals(unassigned, map.get("unassigned"));
            assertEquals(assigned, map.get("assigned"));

            List<String> xContentUnassignedShardIds = (List<String>) map.get("unassigned_shards");
            assertEquals(
                unassignedShardIds.stream()
                    .map(ShardId::toString)
                    .limit(ReactiveStorageDeciderService.ReactiveReason.MAX_AMOUNT_OF_SHARDS)
                    .toList(),
                xContentUnassignedShardIds
            );
            assertSorted(xContentUnassignedShardIds.stream().map(ShardId::fromString).toList());
            assertEquals(unassignedShardIds.size(), map.get("unassigned_shards_count"));

            List<String> xContentAssignedShardIds = (List<String>) map.get("assigned_shards");
            assertEquals(
                assignedShardIds.stream()
                    .map(ShardId::toString)
                    .limit(ReactiveStorageDeciderService.ReactiveReason.MAX_AMOUNT_OF_SHARDS)
                    .collect(Collectors.toList()),
                xContentAssignedShardIds
            );
            assertSorted(xContentAssignedShardIds.stream().map(ShardId::fromString).toList());
            assertEquals(assignedShardIds.size(), map.get("assigned_shards_count"));

            List<Map<String, Object>> unassignedAllocateResults = (List<Map<String, Object>>) map.get("unassigned_allocation_results");
            assertEquals("node1", unassignedAllocateResults.get(0).get("node_id"));
            Map<String, Object> unassignedDeciders = (Map<String, Object>) unassignedAllocateResults.get(0).get("node_decision");
            assertEquals("NO", unassignedDeciders.get("decision"));
            assertEquals("no_label", unassignedDeciders.get("decider"));
            assertEquals("No space to allocate", unassignedDeciders.get("explanation"));

            List<Map<String, Object>> assignedAllocateResults = (List<Map<String, Object>>) map.get("assigned_allocation_results");
            assertEquals("node1", assignedAllocateResults.get(0).get("node_id"));
            Map<String, Object> assignedDeciders = (Map<String, Object>) assignedAllocateResults.get(0).get("node_decision");
            assertEquals("YES", assignedDeciders.get("decision"));
            assertEquals("yes_label", assignedDeciders.get("decider"));
            assertEquals("There's enough space", assignedDeciders.get("explanation"));
        }
    }

    private static void assertSorted(Collection<ShardId> collection) {
        ShardId previous = null;
        for (ShardId e : collection) {
            if (previous != null) {
                assertThat(e, greaterThan(previous));
            }
            previous = e;
        }
    }
}
