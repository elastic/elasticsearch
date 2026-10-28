/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.object.HasToString.hasToString;

public class NodeIndicesStatsTests extends ESTestCase {

    public void testInvalidLevel() {
        final NodeIndicesStats stats = new NodeIndicesStats(
            null,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            randomBoolean()
        );
        final String level = randomAlphaOfLength(16);
        final ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap("level", level));
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stats.toXContentChunked(params));
        assertThat(
            e,
            hasToString(containsString("level parameter must be one of [node] or [indices] or [shards] but was [" + level + "]"))
        );
    }

    public void testIncludeShardsStatsFlag() {
        final Index index = new Index("test", "_na_");
        final Map<Index, List<IndexShardStats>> statsByShards = new HashMap<>();
        final List<IndexShardStats> emptyList = List.of();
        statsByShards.put(index, emptyList);
        NodeIndicesStats stats = new NodeIndicesStats(null, Collections.emptyMap(), statsByShards, Collections.emptyMap(), true);
        assertThat(stats.getShardStats(index), sameInstance(emptyList));
        stats = new NodeIndicesStats(null, Collections.emptyMap(), statsByShards, Collections.emptyMap(), false);
        assertThat(stats.getShardStats(index), nullValue());
    }

}
