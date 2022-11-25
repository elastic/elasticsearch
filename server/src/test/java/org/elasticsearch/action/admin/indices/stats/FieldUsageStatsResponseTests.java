/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.search.stats.FieldUsageStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FieldUsageStatsResponseTests extends ESTestCase {

    public void testToXContentChunkPerIndex() throws IOException {
        final int indices = randomIntBetween(0, 100);
        final Map<String, List<FieldUsageShardResponse>> perIndex = Maps.newMapWithExpectedSize(indices);
        for (int i = 0; i < indices; i++) {
            perIndex.put(
                "index-" + i,
                List.of(
                    new FieldUsageShardResponse(
                        "tracking_id",
                        TestShardRouting.newShardRouting(
                            new ShardId("index" + i, UUIDs.randomBase64UUID(random()), 0),
                            "node_id",
                            true,
                            ShardRoutingState.STARTED
                        ),
                        0,
                        new FieldUsageStats()
                    )
                )
            );
        }
        final FieldUsageStatsResponse response = new FieldUsageStatsResponse(indices, indices, 0, List.of(), perIndex);

        final XContentBuilder builder = JsonXContent.contentBuilder();
        final var iterator = response.toXContentChunked(ToXContent.EMPTY_PARAMS);
        int chunks = 0;
        while (iterator.hasNext()) {
            iterator.next().toXContent(builder, ToXContent.EMPTY_PARAMS);
            chunks++;
        }
        assertEquals(indices + 2, chunks);
    }
}
