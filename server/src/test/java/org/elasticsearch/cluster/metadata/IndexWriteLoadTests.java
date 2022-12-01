/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexWriteLoadTests extends ESTestCase {

    public void testGetWriteLoadForShardAndGetUptimeInMillisForShard() {
        final int numberOfPopulatedShards = 10;
        final int numberOfShards = randomIntBetween(numberOfPopulatedShards, 20);
        final IndexWriteLoad.Builder indexWriteLoadBuilder = IndexWriteLoad.builder(numberOfShards);

        final double[] populatedShardWriteLoads = new double[numberOfPopulatedShards];
        final long[] populatedShardUptimes = new long[numberOfPopulatedShards];
        for (int shardId = 0; shardId < numberOfPopulatedShards; shardId++) {
            double writeLoad = randomDoubleBetween(1, 128, true);
            long uptimeInMillis = randomNonNegativeLong();
            populatedShardWriteLoads[shardId] = writeLoad;
            populatedShardUptimes[shardId] = uptimeInMillis;
            indexWriteLoadBuilder.withShardWriteLoad(shardId, writeLoad, uptimeInMillis);
        }

        final IndexWriteLoad indexWriteLoad = indexWriteLoadBuilder.build();
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            if (shardId < numberOfPopulatedShards) {
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId).isPresent(), is(equalTo(true)));
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId).getAsDouble(), is(equalTo(populatedShardWriteLoads[shardId])));
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId).isPresent(), is(equalTo(true)));
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId).getAsLong(), is(equalTo(populatedShardUptimes[shardId])));
            } else {
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId).isPresent(), is(false));
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId).isPresent(), is(false));
            }
        }
    }
}
