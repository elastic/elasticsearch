/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class IndexMetaDataTests extends ESTestCase {

    public void testIndexMetaDataSerialization() throws IOException {
        Integer numShard = randomFrom(1, 2, 4, 8, 16);
        int numberOfReplicas = randomIntBetween(0, 10);
        IndexMetaData metaData = IndexMetaData.builder("foo")
            .settings(Settings.builder()
                .put("index.version.created", 1)
                .put("index.number_of_shards", numShard)
                .put("index.number_of_replicas", numberOfReplicas)
                .build())
            .creationDate(randomLong())
            .primaryTerm(0, 2)
            .setRoutingNumShards(32)
            .build();

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        metaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, builder.bytes());
        final IndexMetaData fromXContentMeta = IndexMetaData.fromXContent(parser);
        assertEquals(metaData, fromXContentMeta);
        assertEquals(metaData.hashCode(), fromXContentMeta.hashCode());

        assertEquals(metaData.getNumberOfReplicas(), fromXContentMeta.getNumberOfReplicas());
        assertEquals(metaData.getNumberOfShards(), fromXContentMeta.getNumberOfShards());
        assertEquals(metaData.getCreationVersion(), fromXContentMeta.getCreationVersion());
        assertEquals(metaData.getRoutingNumShards(), fromXContentMeta.getRoutingNumShards());
        assertEquals(metaData.getCreationDate(), fromXContentMeta.getCreationDate());
        assertEquals(metaData.getRoutingFactor(), fromXContentMeta.getRoutingFactor());
        assertEquals(metaData.primaryTerm(0), fromXContentMeta.primaryTerm(0));

        final BytesStreamOutput out = new BytesStreamOutput();
        metaData.writeTo(out);
        IndexMetaData deserialized = IndexMetaData.readFrom(out.bytes().streamInput());
        assertEquals(metaData, deserialized);
        assertEquals(metaData.hashCode(), deserialized.hashCode());

        assertEquals(metaData.getNumberOfReplicas(), deserialized.getNumberOfReplicas());
        assertEquals(metaData.getNumberOfShards(), deserialized.getNumberOfShards());
        assertEquals(metaData.getCreationVersion(), deserialized.getCreationVersion());
        assertEquals(metaData.getRoutingNumShards(), deserialized.getRoutingNumShards());
        assertEquals(metaData.getCreationDate(), deserialized.getCreationDate());
        assertEquals(metaData.getRoutingFactor(), deserialized.getRoutingFactor());
        assertEquals(metaData.primaryTerm(0), deserialized.primaryTerm(0));
    }

    public void testGetRoutingFactor() {
        Integer numShard = randomFrom(1, 2, 4, 8, 16);
        int routingFactor = IndexMetaData.getRoutingFactor(32, numShard);
        assertEquals(routingFactor * numShard, 32);

        Integer brokenNumShards = randomFrom(3, 5, 9, 12, 29, 42);
        expectThrows(IllegalArgumentException.class, () -> IndexMetaData.getRoutingFactor(32, brokenNumShards));
    }

    public void testSelectShrinkShards() {
        int numberOfReplicas = randomIntBetween(0, 10);
        IndexMetaData metaData = IndexMetaData.builder("foo")
            .settings(Settings.builder()
                .put("index.version.created", 1)
                .put("index.number_of_shards", 32)
                .put("index.number_of_replicas", numberOfReplicas)
                .build())
            .creationDate(randomLong())
            .build();
        Set<ShardId> shardIds = IndexMetaData.selectShrinkShards(0, metaData, 8);
        assertEquals(shardIds, Sets.newHashSet(new ShardId(metaData.getIndex(), 0), new ShardId(metaData.getIndex(), 1),
            new ShardId(metaData.getIndex(), 2), new ShardId(metaData.getIndex(), 3)));
        shardIds = IndexMetaData.selectShrinkShards(1, metaData, 8);
        assertEquals(shardIds, Sets.newHashSet(new ShardId(metaData.getIndex(), 4), new ShardId(metaData.getIndex(), 5),
            new ShardId(metaData.getIndex(), 6), new ShardId(metaData.getIndex(), 7)));
        shardIds = IndexMetaData.selectShrinkShards(7, metaData, 8);
        assertEquals(shardIds, Sets.newHashSet(new ShardId(metaData.getIndex(), 28), new ShardId(metaData.getIndex(), 29),
            new ShardId(metaData.getIndex(), 30), new ShardId(metaData.getIndex(), 31)));

        assertEquals("the number of target shards (8) must be greater than the shard id: 8",
            expectThrows(IllegalArgumentException.class, () -> IndexMetaData.selectShrinkShards(8, metaData, 8)).getMessage());
    }

    public void testSelectResizeShards() {
        int numTargetShards = randomFrom(4, 6, 8, 12);

        IndexMetaData split = IndexMetaData.builder("foo")
            .settings(Settings.builder()
                .put("index.version.created", 1)
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
                .build())
            .creationDate(randomLong())
            .setRoutingNumShards(numTargetShards * 2)
            .build();

        IndexMetaData shrink = IndexMetaData.builder("foo")
            .settings(Settings.builder()
                .put("index.version.created", 1)
                .put("index.number_of_shards", 32)
                .put("index.number_of_replicas", 0)
                .build())
            .creationDate(randomLong())
            .build();
        int shard = randomIntBetween(0, numTargetShards-1);
        assertEquals(Collections.singleton(IndexMetaData.selectSplitShard(shard, split, numTargetShards)),
            IndexMetaData.selectRecoverFromShards(shard, split, numTargetShards));

        numTargetShards = randomFrom(1, 2, 4, 8, 16);
        shard = randomIntBetween(0, numTargetShards-1);
        assertEquals(IndexMetaData.selectShrinkShards(shard, shrink, numTargetShards),
            IndexMetaData.selectRecoverFromShards(shard, shrink, numTargetShards));

        assertEquals("can't select recover from shards if both indices have the same number of shards",
            expectThrows(IllegalArgumentException.class, () -> IndexMetaData.selectRecoverFromShards(0, shrink, 32)).getMessage());
    }

    public void testSelectSplitShard() {
        IndexMetaData metaData = IndexMetaData.builder("foo")
            .settings(Settings.builder()
                .put("index.version.created", 1)
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
                .build())
            .creationDate(randomLong())
            .setRoutingNumShards(4)
            .build();
        ShardId shardId = IndexMetaData.selectSplitShard(0, metaData, 4);
        assertEquals(0, shardId.getId());
        shardId = IndexMetaData.selectSplitShard(1, metaData, 4);
        assertEquals(0, shardId.getId());
        shardId = IndexMetaData.selectSplitShard(2, metaData, 4);
        assertEquals(1, shardId.getId());
        shardId = IndexMetaData.selectSplitShard(3, metaData, 4);
        assertEquals(1, shardId.getId());

        assertEquals("the number of target shards (0) must be greater than the shard id: 0",
            expectThrows(IllegalArgumentException.class, () -> IndexMetaData.selectSplitShard(0, metaData, 0)).getMessage());

        assertEquals("the number of source shards [2] must be a must be a factor of [3]",
            expectThrows(IllegalArgumentException.class, () -> IndexMetaData.selectSplitShard(0, metaData, 3)).getMessage());

        assertEquals("the number of routing shards [4] must be a multiple of the target shards [8]",
            expectThrows(IllegalStateException.class, () -> IndexMetaData.selectSplitShard(0, metaData, 8)).getMessage());
    }

    public void testIndexFormat() {
        Settings defaultSettings = Settings.builder()
                .put("index.version.created", 1)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .build();

        // matching version
        {
            IndexMetaData metaData = IndexMetaData.builder("foo")
                    .settings(Settings.builder()
                            .put(defaultSettings)
                            // intentionally not using the constant, so upgrading requires you to look at this test
                            // where you have to update this part and the next one
                            .put("index.format", 6)
                            .build())
                    .build();

            assertThat(metaData.getSettings().getAsInt(IndexMetaData.INDEX_FORMAT_SETTING.getKey(), 0), is(6));
        }

        // no setting configured
        {
            IndexMetaData metaData = IndexMetaData.builder("foo")
                    .settings(Settings.builder()
                            .put(defaultSettings)
                            .build())
                    .build();
            assertThat(metaData.getSettings().getAsInt(IndexMetaData.INDEX_FORMAT_SETTING.getKey(), 0), is(0));
        }
    }

    public void testNumberOfRoutingShards() {
        Settings build = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_routing_shards", 10).build();
        assertEquals(10, IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(build).intValue());

        build = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_routing_shards", 5).build();
        assertEquals(5, IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(build).intValue());

        int numShards = randomIntBetween(1, 10);
        build = Settings.builder().put("index.number_of_shards", numShards).build();
        assertEquals(numShards, IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(build).intValue());

        Settings lessThanSettings = Settings.builder().put("index.number_of_shards", 8).put("index.number_of_routing_shards", 4).build();
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
            () -> IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(lessThanSettings));
        assertEquals("index.number_of_routing_shards [4] must be >= index.number_of_shards [8]", iae.getMessage());

        Settings notAFactorySettings = Settings.builder().put("index.number_of_shards", 2).put("index.number_of_routing_shards", 3).build();
        iae = expectThrows(IllegalArgumentException.class,
            () -> IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(notAFactorySettings));
        assertEquals("the number of source shards [2] must be a must be a factor of [3]", iae.getMessage());
    }
}
