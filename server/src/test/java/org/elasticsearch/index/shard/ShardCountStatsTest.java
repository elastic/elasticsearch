/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.junit.Assert;

class ShardCountStatsTest extends AbstractWireSerializingTestCase<ShardCountStats> {

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

    @Override
    protected Writeable.Reader<ShardCountStats> instanceReader() {
        return ShardCountStats::new;
    }

    @Override
    protected ShardCountStats createTestInstance() {
        return new ShardCountStats(randomIntBetween(0, Integer.MAX_VALUE));
    }
}
