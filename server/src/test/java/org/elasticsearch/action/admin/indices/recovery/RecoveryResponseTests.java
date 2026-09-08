/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class RecoveryResponseTests extends ESTestCase {

    public void testRecoveryInfosMustNotBeNull() {
        expectThrows(NullPointerException.class, () -> new RecoveryResponse(0, 0, 0, null, List.of()));
    }

    public void testChunkedToXContent() {
        final int failedShards = randomIntBetween(0, 50);
        final int successfulShards = randomIntBetween(0, 50);
        DiscoveryNode sourceNode = DiscoveryNodeUtils.builder("foo").roles(emptySet()).build();
        DiscoveryNode targetNode = DiscoveryNodeUtils.builder("bar").roles(emptySet()).build();
        final int shards = randomInt(50);
        AbstractChunkedSerializingTestCase.assertChunkCount(
            new RecoveryResponse(
                successfulShards + failedShards,
                successfulShards,
                failedShards,
                IntStream.range(0, shards)
                    .boxed()
                    .collect(
                        Collectors.toUnmodifiableMap(
                            i -> "index-" + i,
                            i -> List.of(
                                new ShardRecoveryInfo(
                                    new RecoveryState(
                                        ShardRouting.newUnassigned(
                                            new ShardId("index-" + i, "index-uuid-" + i, 0),
                                            false,
                                            RecoverySource.PeerRecoverySource.INSTANCE,
                                            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
                                            ShardRouting.Role.DEFAULT,
                                            ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
                                        ).initialize(sourceNode.getId(), null, randomNonNegativeLong()),
                                        sourceNode,
                                        targetNode
                                    ),
                                    null
                                )
                            )
                        )
                    ),
                List.of()
            ),
            ignored -> shards + 2
        );
    }

    public void testGateIsRenderedOnlyForDeferredRecoveries() throws IOException {
        final String indexName = randomIndexName();
        final RecoveryState gatedState = createRecoveryState(indexName, 0);
        final RecoveryState ungatedState = createRecoveryState(indexName, 1);
        final String gate = randomIdentifier();
        final RecoveryResponse response = new RecoveryResponse(
            2,
            2,
            0,
            Map.of(indexName, List.of(new ShardRecoveryInfo(gatedState, gate), new ShardRecoveryInfo(ungatedState, null))),
            List.of()
        );

        final Map<String, Object> responseMap;
        try (var builder = JsonXContent.contentBuilder()) {
            ChunkedToXContent.wrapAsToXContent(response).toXContent(builder, ToXContent.EMPTY_PARAMS);
            responseMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
        final Map<?, ?> index = (Map<?, ?>) responseMap.get(indexName);
        final List<?> shards = (List<?>) index.get("shards");
        final Map<?, ?> gatedShard = (Map<?, ?>) shards.get(0);
        final Map<?, ?> ungatedShard = (Map<?, ?>) shards.get(1);
        assertThat(gatedShard.get("gate"), equalTo(gate));
        assertFalse(ungatedShard.containsKey("gate"));
    }

    public void testGateTransportSerialization() throws IOException {
        final String gate = randomIdentifier();
        final ShardRecoveryInfo recoveryInfo = new ShardRecoveryInfo(createRecoveryState(randomIndexName(), 0), gate);

        final ShardRecoveryInfo oldVersionCopy = copyWriteable(
            recoveryInfo,
            writableRegistry(),
            ShardRecoveryInfo::new,
            TransportVersionUtils.getPreviousVersion(ShardRecoveryInfo.GATE_IN_RECOVERY_RESPONSE)
        );
        assertNull(oldVersionCopy.gate());

        final ShardRecoveryInfo supportingVersionCopy = copyWriteable(
            recoveryInfo,
            writableRegistry(),
            ShardRecoveryInfo::new,
            TransportVersionUtils.randomVersionSupporting(ShardRecoveryInfo.GATE_IN_RECOVERY_RESPONSE)
        );
        assertThat(supportingVersionCopy.gate(), equalTo(gate));
    }

    private static RecoveryState createRecoveryState(String indexName, int shardId) {
        final DiscoveryNode sourceNode = DiscoveryNodeUtils.builder(randomIdentifier()).roles(emptySet()).build();
        final DiscoveryNode targetNode = DiscoveryNodeUtils.builder(randomIdentifier()).roles(emptySet()).build();
        return new RecoveryState(
            ShardRouting.newUnassigned(
                new ShardId(indexName, randomUUID(), shardId),
                false,
                RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
                ShardRouting.Role.DEFAULT,
                ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
            ).initialize(sourceNode.getId(), null, randomNonNegativeLong()),
            sourceNode,
            targetNode
        );
    }
}
