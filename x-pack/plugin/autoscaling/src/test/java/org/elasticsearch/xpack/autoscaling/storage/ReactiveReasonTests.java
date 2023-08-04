/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        DiscoveryNode discoveryNode2 = DiscoveryNodeUtils.builder("node2").roles(emptySet()).build();
        Map<ShardId, NodeDecisions> unassignedShardAllocationDecision = Map.of(
            unassignedShardIds.first(),
            new NodeDecisions(
                List.of(
                    new NodeDecision(discoveryNode, Decision.single(Decision.Type.NO, "no_label", "No space to allocate")),
                    new NodeDecision(
                        discoveryNode2,
                        new Decision.Multi().add(
                            Decision.single(Decision.Type.YES, "data_tier", "Enough disk on this node for the shard to remain")
                        ).add(Decision.single(Decision.Type.NO, "shards_limit", "Disallowed because of shard limits"))
                    )
                ),
                null
            )
        );
        Map<ShardId, NodeDecisions> assignedShardAllocateDecision = Map.of(
            assignedShardIds.first(),
            new NodeDecisions(
                List.of(),
                new NodeDecision(
                    discoveryNode,
                    new Decision.Multi().add(Decision.single(Decision.Type.THROTTLE, "multi_throttle", "is not active yet"))
                        .add(new Decision.Single(Decision.Type.NO, "multi_no", "No multi decision"))
                        .add(new Decision.Single(Decision.Type.YES, "multi_yes", "Yes multi decision"))
                )
            )
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

            Map<String, Object> unassignedNodeDecisions = (Map<String, Object>) ((Map<String, Object>) map.get("unassigned_node_decisions"))
                .get(unassignedShardIds.first().toString());
            List<Map<String, Object>> canAllocateDecisions = (List<Map<String, Object>>) unassignedNodeDecisions.get(
                "can_allocate_decisions"
            );
            assertEquals(2, canAllocateDecisions.size());
            assertEquals("node1", canAllocateDecisions.get(0).get("node_id"));
            assertEquals(
                List.of(Map.of("decision", "NO", "decider", "no_label", "explanation", "No space to allocate")),
                canAllocateDecisions.get(0).get("deciders")
            );
            assertEquals("node2", canAllocateDecisions.get(1).get("node_id"));
            assertEquals(
                List.of(
                    Map.of("decision", "YES", "decider", "data_tier", "explanation", "Enough disk on this node for the shard to remain"),
                    Map.of("decision", "NO", "decider", "shards_limit", "explanation", "Disallowed because of shard limits")
                ),
                canAllocateDecisions.get(1).get("deciders")
            );
            assertFalse(unassignedNodeDecisions.containsKey("can_remain_decision"));

            Map<String, Object> assignedNodeDecisions = (Map<String, Object>) ((Map<String, Object>) map.get("assigned_node_decisions"))
                .get(assignedShardIds.first().toString());
            var canRemainDecision = (Map<String, Object>) assignedNodeDecisions.get("can_remain_decision");
            assertEquals("node1", canRemainDecision.get("node_id"));
            assertEquals(
                List.of(
                    Map.of("decision", "THROTTLE", "decider", "multi_throttle", "explanation", "is not active yet"),
                    Map.of("decision", "NO", "decider", "multi_no", "explanation", "No multi decision"),
                    Map.of("decision", "YES", "decider", "multi_yes", "explanation", "Yes multi decision")
                ),
                canRemainDecision.get("deciders")
            );
            assertEquals(List.of(), assignedNodeDecisions.get("can_allocate_decisions"));
        }
    }

    public void testEmptyNodeDecisions() throws IOException {
        var reactiveReason = new ReactiveStorageDeciderService.ReactiveReason(
            randomAlphaOfLength(10),
            randomNonNegativeLong(),
            Collections.emptySortedSet(),
            randomNonNegativeLong(),
            Collections.emptySortedSet(),
            Map.of(),
            Map.of()
        );
        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                BytesReference.bytes(reactiveReason.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            )
        ) {
            Map<String, Object> map = parser.map();
            assertEquals(Map.of(), map.get("unassigned_node_decisions"));
            assertEquals(Map.of(), map.get("assigned_node_decisions"));
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
