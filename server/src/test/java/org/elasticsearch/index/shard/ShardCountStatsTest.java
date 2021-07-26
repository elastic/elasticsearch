/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import static org.hamcrest.Matchers.equalTo;

class ShardCountStatsTest extends ESTestCase {

    public void testAdd() {
        ShardCountStats shardStats1 = new ShardCountStats(5);
        ShardCountStats shardStats2 = new ShardCountStats(8);
        ShardCountStats combinedShardStats = shardStats1.add(shardStats2);
        Assert.assertEquals(13, combinedShardStats.getTotalCount());
        Assert.assertEquals(5, shardStats1.getTotalCount());
        Assert.assertEquals(8, shardStats2.getTotalCount());
        ShardCountStats noCountGiven = new ShardCountStats();
        Assert.assertEquals(0, noCountGiven.getTotalCount());
        Assert.assertEquals(8, shardStats2.add(null).getTotalCount());
        Assert.assertEquals(8, shardStats2.getTotalCount());
    }

    public void testSerialize() throws Exception {
        ShardCountStats originalStats = new ShardCountStats(5);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStats.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = bytes.streamInput()) {
                ShardCountStats cloneStats = new ShardCountStats(in);
                assertThat(cloneStats.getTotalCount(), equalTo(originalStats.getTotalCount()));
            }
        }
    }
}
