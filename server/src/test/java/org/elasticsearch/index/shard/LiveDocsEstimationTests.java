/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class LiveDocsEstimationTests extends IndexShardTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
    }

    public void testShardFieldStatsWithDeletes() throws IOException {
        Settings settings = Settings.builder()
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
            .build();
        IndexShard shard = newShard(true, settings);
        assertNull(shard.getShardFieldStats());
        recoverShardFromStore(shard);

        // Use enough docs so the live docs FixedBitSet backing array requires multiple words (>64 bits),
        // ensuring the byte estimation scales with segment size rather than being a constant.
        int numDocs = randomIntBetween(100, 1000);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(shard, "_doc", "first_" + i, """
                {
                    "f1": "foo",
                    "f2": "bar"
                }
                """);
        }
        shard.refresh("test");
        var stats = shard.getShardFieldStats();
        assertThat(stats.numSegments(), equalTo(1));
        assertThat(stats.liveDocsBytes(), equalTo(0L));

        // delete a doc
        deleteDoc(shard, "first_0");

        // Refresh and fetch new stats:
        shard.refresh("test");
        stats = shard.getShardFieldStats();
        // More segments because delete operation is stored in the new segment for replication purposes.
        assertThat(stats.numSegments(), equalTo(2));
        long expectedLiveDocsSize = 0;
        // Delete op is stored in new segment, but marked as deleted. All segements have live docs:
        expectedLiveDocsSize += new FixedBitSet(numDocs).ramBytesUsed();
        // Second segment the delete operation that is marked as deleted:
        expectedLiveDocsSize += new FixedBitSet(1).ramBytesUsed();
        assertThat(stats.liveDocsBytes(), equalTo(expectedLiveDocsSize));

        // delete another doc:
        deleteDoc(shard, "first_1");
        shard.getMinRetainedSeqNo();

        // Refresh and fetch new stats:
        shard.refresh("test");
        stats = shard.getShardFieldStats();
        // More segments because delete operation is stored in the new segment for replication purposes.
        assertThat(stats.numSegments(), equalTo(3));
        expectedLiveDocsSize = 0;
        // Delete op is stored in new segment, but marked as deleted. All segements have live docs:
        // First segment with deletes
        expectedLiveDocsSize += new FixedBitSet(numDocs).ramBytesUsed();
        // Second and third segments the delete operation that is marked as deleted:
        expectedLiveDocsSize += new FixedBitSet(1).ramBytesUsed();
        expectedLiveDocsSize += new FixedBitSet(1).ramBytesUsed();
        assertThat(stats.liveDocsBytes(), equalTo(expectedLiveDocsSize));

        closeShards(shard);
    }

}
