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

class SimpleShardStatsTest extends ESTestCase {

    public void testAdd() {
        SimpleShardStats shardStats1 = new SimpleShardStats(5);
        SimpleShardStats shardStats2 = new SimpleShardStats(8);
        SimpleShardStats combinedShardStats = shardStats1.add(shardStats2);
        Assert.assertEquals(13, combinedShardStats.getCount());
        Assert.assertEquals(5, shardStats1.getCount());
        Assert.assertEquals(8, shardStats2.getCount());
        SimpleShardStats noCountGiven = new SimpleShardStats();
        Assert.assertEquals(0, noCountGiven.getCount());
        Assert.assertEquals(8, shardStats2.add(null).getCount());
        Assert.assertEquals(8, shardStats2.getCount());
    }

    public void testSerialize() throws Exception {
        SimpleShardStats originalStats = new SimpleShardStats(5);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStats.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = bytes.streamInput()) {
                SimpleShardStats cloneStats = new SimpleShardStats(in);
                assertThat(cloneStats.getCount(), equalTo(originalStats.getCount()));
            }
        }
    }
}
