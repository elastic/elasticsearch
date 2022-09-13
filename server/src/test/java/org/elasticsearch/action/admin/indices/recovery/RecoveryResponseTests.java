/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class RecoveryResponseTests extends ESTestCase {

    public void testChunkedToXContent() {
        final int failedShards = randomIntBetween(0, 50);
        final int successfulShards = randomIntBetween(0, 50);
        DiscoveryNode sourceNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode targetNode = new DiscoveryNode("bar", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final int shards = randomInt(50);
        final RecoveryResponse recoveryResponse = new RecoveryResponse(
            successfulShards + failedShards,
            successfulShards,
            failedShards,
            IntStream.range(0, shards)
                .boxed()
                .collect(
                    Collectors.toUnmodifiableMap(
                        i -> "index-" + i,
                        i -> List.of(
                            new RecoveryState(
                                ShardRouting.newUnassigned(
                                    new ShardId("index-" + i, "index-uuid-" + i, 0),
                                    randomBoolean(),
                                    RecoverySource.PeerRecoverySource.INSTANCE,
                                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
                                ).initialize(sourceNode.getId(), null, randomNonNegativeLong()),
                                sourceNode,
                                targetNode
                            )
                        )
                    )
                ),
            List.of()
        );
        final var iterator = recoveryResponse.toXContentChunked();
        int chunks = 0;
        while (iterator.hasNext()) {
            iterator.next();
            chunks++;
        }
        assertEquals(shards + 2, chunks);
    }
}
