/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceStats;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DesiredBalanceResponseTests extends AbstractWireSerializingTestCase<DesiredBalanceResponse> {

    @Override
    protected Writeable.Reader<DesiredBalanceResponse> instanceReader() {
        return DesiredBalanceResponse::from;
    }

    @Override
    protected DesiredBalanceResponse createTestInstance() {
        return new DesiredBalanceResponse(randomStats(), randomRoutingTable());
    }

    private DesiredBalanceStats randomStats() {
        return new DesiredBalanceStats(
            randomNonNegativeLong(),
            randomBoolean(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    private Map<String, Map<Integer, DesiredBalanceResponse.DesiredShards>> randomRoutingTable() {
        Map<String, Map<Integer, DesiredBalanceResponse.DesiredShards>> routingTable = new HashMap<>();
        for (int i = 0; i < randomInt(8); i++) {
            String indexName = randomAlphaOfLength(8);
            Map<Integer, DesiredBalanceResponse.DesiredShards> desiredShards = new HashMap<>();
            for (int j = 0; j < randomInt(8); j++) {
                int shardId = randomInt(1024);
                desiredShards.put(
                    shardId,
                    new DesiredBalanceResponse.DesiredShards(
                        IntStream.range(0, randomIntBetween(1, 4))
                            .mapToObj(
                                k -> new DesiredBalanceResponse.ShardView(
                                    randomFrom(ShardRoutingState.STARTED, ShardRoutingState.UNASSIGNED, ShardRoutingState.INITIALIZING),
                                    randomBoolean(),
                                    randomAlphaOfLength(8),
                                    randomBoolean(),
                                    randomAlphaOfLength(8),
                                    randomBoolean(),
                                    shardId,
                                    indexName,
                                    randomBoolean() ? randomDouble() : null,
                                    randomBoolean() ? randomLong() : null
                                )
                            )
                            .toList(),
                        new DesiredBalanceResponse.ShardAssignmentView(
                            randomUnique(() -> randomAlphaOfLength(8), randomIntBetween(1, 8)),
                            randomInt(8),
                            randomInt(8),
                            randomInt(8)
                        )
                    )
                );
            }
            routingTable.put(indexName, Collections.unmodifiableMap(desiredShards));
        }
        return Collections.unmodifiableMap(routingTable);
    }

    @Override
    protected DesiredBalanceResponse mutateInstance(DesiredBalanceResponse instance) throws IOException {
        return switch (randomInt(2)) {
            case 0 -> new DesiredBalanceResponse(
                instance.getStats(),
                randomValueOtherThan(instance.getRoutingTable(), this::randomRoutingTable)
            );
            case 1 -> new DesiredBalanceResponse(randomStats(), instance.getRoutingTable());
            default -> randomValueOtherThan(instance, this::createTestInstance);
        };
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        DesiredBalanceResponse response = new DesiredBalanceResponse(randomStats(), randomRoutingTable());

        Map<String, Object> json = createParser(
            ChunkedToXContent.wrapAsToXContent(response).toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
        ).map();
        assertEquals(Set.of("stats", "routing_table"), json.keySet());

        Map<String, Object> stats = (Map<String, Object>) json.get("stats");
        assertEquals(stats.get("computation_active"), response.getStats().computationActive());
        assertEquals(stats.get("computation_submitted"), response.getStats().computationSubmitted());
        assertEquals(stats.get("computation_executed"), response.getStats().computationExecuted());
        assertEquals(stats.get("computation_converged"), response.getStats().computationConverged());
        assertEquals(stats.get("computation_iterations"), response.getStats().computationIterations());
        assertEquals(stats.get("computation_converged_index"), response.getStats().lastConvergedIndex());
        assertEquals(stats.get("computation_time_in_millis"), response.getStats().cumulativeComputationTime());
        assertEquals(stats.get("reconciliation_time_in_millis"), response.getStats().cumulativeReconciliationTime());

        Map<String, Object> jsonRoutingTable = (Map<String, Object>) json.get("routing_table");
        assertEquals(jsonRoutingTable.keySet(), response.getRoutingTable().keySet());
        for (var indexEntry : response.getRoutingTable().entrySet()) {
            Map<String, Object> jsonIndexShards = (Map<String, Object>) jsonRoutingTable.get(indexEntry.getKey());
            assertEquals(
                jsonIndexShards.keySet(),
                indexEntry.getValue().keySet().stream().map(String::valueOf).collect(Collectors.toSet())
            );
            for (var shardEntry : indexEntry.getValue().entrySet()) {
                DesiredBalanceResponse.DesiredShards desiredShards = shardEntry.getValue();
                Map<String, Object> jsonDesiredShard = (Map<String, Object>) jsonIndexShards.get(String.valueOf(shardEntry.getKey()));
                assertEquals(Set.of("current", "desired"), jsonDesiredShard.keySet());
                List<Map<String, Object>> jsonCurrent = (List<Map<String, Object>>) jsonDesiredShard.get("current");
                for (int i = 0; i < jsonCurrent.size(); i++) {
                    Map<String, Object> jsonShard = jsonCurrent.get(i);
                    DesiredBalanceResponse.ShardView shardView = desiredShards.current().get(i);
                    assertEquals(jsonShard.get("state"), shardView.state().toString());
                    assertEquals(jsonShard.get("primary"), shardView.primary());
                    assertEquals(jsonShard.get("node"), shardView.node());
                    assertEquals(jsonShard.get("node_is_desired"), shardView.nodeIsDesired());
                    assertEquals(jsonShard.get("relocating_node"), shardView.relocatingNode());
                    assertEquals(jsonShard.get("relocating_node_is_desired"), shardView.relocatingNodeIsDesired());
                    assertEquals(jsonShard.get("shard_id"), shardView.shardId());
                    assertEquals(jsonShard.get("index"), shardView.index());
                    assertEquals(jsonShard.get("forecasted_write_load"), shardView.forecastedWriteLoad());
                    assertEquals(jsonShard.get("forecasted_shard_size_in_bytes"), shardView.forecastedShardSizeInBytes());
                }

                Map<String, Object> jsonDesired = (Map<String, Object>) jsonDesiredShard.get("desired");
                List<String> nodeIds = (List<String>) jsonDesired.get("node_ids");
                assertEquals(nodeIds, List.copyOf(desiredShards.desired().nodeIds()));
                assertEquals(jsonDesired.get("total"), desiredShards.desired().total());
                assertEquals(jsonDesired.get("unassigned"), desiredShards.desired().unassigned());
                assertEquals(jsonDesired.get("ignored"), desiredShards.desired().ignored());
            }
        }
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(
            new DesiredBalanceResponse(randomStats(), randomRoutingTable()),
            response -> response.getRoutingTable().size() + 2
        );
    }
}
