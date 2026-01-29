/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class IndexWriteLoadTests extends ESTestCase {

    public void testGetWriteLoadForShardAndGetUptimeInMillisForShard() {
        final int numberOfPopulatedShards = 10;
        final int numberOfShards = randomIntBetween(numberOfPopulatedShards, 20);
        final IndexWriteLoad.Builder indexWriteLoadBuilder = IndexWriteLoad.builder(numberOfShards);

        final double[] populatedShardWriteLoads = new double[numberOfPopulatedShards];
        final double[] populatedShardRecentWriteLoads = new double[numberOfPopulatedShards];
        final double[] populatedShardPeakWriteLoads = new double[numberOfPopulatedShards];
        final long[] populatedShardUptimes = new long[numberOfPopulatedShards];
        for (int shardId = 0; shardId < numberOfPopulatedShards; shardId++) {
            double writeLoad = randomDoubleBetween(1, 128, true);
            double recentWriteLoad = randomDoubleBetween(1, 128, true);
            double peakWriteLoad = randomDoubleBetween(1, 128, true);
            long uptimeInMillis = randomNonNegativeLong();
            populatedShardWriteLoads[shardId] = writeLoad;
            populatedShardRecentWriteLoads[shardId] = recentWriteLoad;
            populatedShardPeakWriteLoads[shardId] = peakWriteLoad;
            populatedShardUptimes[shardId] = uptimeInMillis;
            indexWriteLoadBuilder.withShardWriteLoad(shardId, writeLoad, recentWriteLoad, peakWriteLoad, uptimeInMillis);
        }

        final IndexWriteLoad indexWriteLoad = indexWriteLoadBuilder.build();
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            if (shardId < numberOfPopulatedShards) {
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId), equalTo(OptionalDouble.of(populatedShardWriteLoads[shardId])));
                assertThat(
                    indexWriteLoad.getRecentWriteLoadForShard(shardId),
                    equalTo(OptionalDouble.of(populatedShardRecentWriteLoads[shardId]))
                );
                assertThat(
                    indexWriteLoad.getPeakWriteLoadForShard(shardId),
                    equalTo(OptionalDouble.of(populatedShardPeakWriteLoads[shardId]))
                );
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId), equalTo(OptionalLong.of(populatedShardUptimes[shardId])));
            } else {
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId), equalTo(OptionalDouble.empty()));
                assertThat(indexWriteLoad.getRecentWriteLoadForShard(shardId), equalTo(OptionalDouble.empty()));
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId), equalTo(OptionalLong.empty()));
            }
        }
    }

    public void testXContent_roundTrips() throws IOException {
        IndexWriteLoad original = randomIndexWriteLoad();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().value(original).endObject();
        IndexWriteLoad roundTripped = IndexWriteLoad.fromXContent(createParser(xContentBuilder));
        assertThat(roundTripped, equalTo(original));
    }

    public void testXContent_missingRecentWriteAndPeakLoad() throws IOException {
        // Simulate a JSON serialization from before we added recent write load:
        IndexWriteLoad original = randomIndexWriteLoad();
        XContentBuilder builder = XContentBuilder.builder(
            XContentType.JSON,
            emptySet(),
            Set.of(
                IndexWriteLoad.SHARDS_RECENT_WRITE_LOAD_FIELD.getPreferredName(),
                IndexWriteLoad.SHARDS_PEAK_WRITE_LOAD_FIELD.getPreferredName()
            )
        ).startObject().value(original).endObject();
        // Deserialize, and assert that it matches the original except the recent write loads are all missing:
        IndexWriteLoad roundTripped = IndexWriteLoad.fromXContent(createParser(builder));
        for (int i = 0; i < original.numberOfShards(); i++) {
            assertThat(roundTripped.getUptimeInMillisForShard(i), equalTo(original.getUptimeInMillisForShard(i)));
            assertThat(roundTripped.getWriteLoadForShard(i), equalTo(original.getWriteLoadForShard(i)));
            assertThat(roundTripped.getRecentWriteLoadForShard(i), equalTo(OptionalDouble.empty()));
            assertThat(roundTripped.getPeakWriteLoadForShard(i), equalTo(OptionalDouble.empty()));
        }
    }

    private static IndexWriteLoad randomIndexWriteLoad() {
        int numberOfPopulatedShards = 10;
        int numberOfShards = randomIntBetween(numberOfPopulatedShards, 20);
        IndexWriteLoad.Builder builder = IndexWriteLoad.builder(numberOfShards);
        for (int shardId = 0; shardId < numberOfPopulatedShards; shardId++) {
            builder.withShardWriteLoad(
                shardId,
                randomDoubleBetween(1, 128, true),
                randomDoubleBetween(1, 128, true),
                randomDoubleBetween(1, 128, true),
                randomNonNegativeLong()
            );
        }
        return builder.build();
    }
}
