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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Set;

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
        XContentParser parser = XContentType.JSON.xContent().createParser(builder.bytes());
        final IndexMetaData fromXContentMeta = IndexMetaData.PROTO.fromXContent(parser, null);
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
        IndexMetaData deserialized = IndexMetaData.PROTO.readFrom(StreamInput.wrap(out.bytes()));
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
        int numberOfReplicas = randomIntBetween(0, 10);
        IndexMetaData metaData = IndexMetaData.builder("foo")
            .settings(Settings.builder()
                .put("index.version.created", 1)
                .put("index.number_of_shards", 32)
                .put("index.number_of_replicas", numberOfReplicas)
                .build())
            .creationDate(randomLong())
            .build();
        Integer numShard = randomFrom(1, 2, 4, 8, 16);
        int routingFactor = IndexMetaData.getRoutingFactor(metaData, numShard);
        assertEquals(routingFactor * numShard, metaData.getNumberOfShards());

        Integer brokenNumShards = randomFrom(3, 5, 9, 12, 29, 42, 64);
        expectThrows(IllegalArgumentException.class, () -> IndexMetaData.getRoutingFactor(metaData, brokenNumShards));
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
}
