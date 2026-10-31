/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Splitting left pages by the right shard that holds their matches. The shards are checked against how the right index
 * routes a document whose routing field has the same value, which is what the join relies on.
 */
public class PartnerShardsTests extends ESTestCase {
    private static final int SHARDS = 5;

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testKeywordKeysRouteLikeDocuments() {
        IndexRouting routing = routing("keyword");
        RoutingField field = new RoutingField("k", RoutingField.Type.KEYWORD);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            keys.add(randomAlphaOfLengthBetween(1, 10));
        }
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(keys.size())) {
            keys.forEach(k -> builder.appendBytesRef(new BytesRef(k)));
            List<PartnerShards.Group> groups = partners(PartnerShards.KeyType.KEYWORD, routing).split(new Page(builder.build()));
            int[] shards = shardsOf(groups, keys.size());
            for (int p = 0; p < keys.size(); p++) {
                assertThat(keys.get(p), shards[p], equalTo(documentShard(routing, field, "\"" + keys.get(p) + "\"")));
            }
        }
    }

    /**
     * An {@code integer} left key and a {@code long} routing field with the same value route alike.
     */
    public void testIntegralKeysRouteLikeDocuments() {
        IndexRouting routing = routing("long");
        RoutingField field = new RoutingField("k", RoutingField.Type.INTEGRAL);
        int[] values = new int[50];
        try (IntBlock.Builder ints = blockFactory.newIntBlockBuilder(values.length)) {
            for (int i = 0; i < values.length; i++) {
                values[i] = randomInt();
                ints.appendInt(values[i]);
            }
            int[] shards = shardsOf(partners(PartnerShards.KeyType.INTEGER, routing).split(new Page(ints.build())), values.length);
            for (int p = 0; p < values.length; p++) {
                assertThat(shards[p], equalTo(documentShard(routing, field, Integer.toString(values[p]))));
            }
        }
        try (LongBlock.Builder longs = blockFactory.newLongBlockBuilder(values.length)) {
            for (int value : values) {
                longs.appendLong(value);
            }
            int[] shards = shardsOf(partners(PartnerShards.KeyType.LONG, routing).split(new Page(longs.build())), values.length);
            for (int p = 0; p < values.length; p++) {
                assertThat(shards[p], equalTo(documentShard(routing, field, "\"" + values[p] + "\"")));
            }
        }
    }

    /**
     * Keys are read in their indexed form, so an IPv4-mapped IPv6 address routes like its IPv4 form.
     */
    public void testIpKeysRouteLikeDocuments() {
        IndexRouting routing = routing("ip");
        RoutingField field = new RoutingField("k", RoutingField.Type.IP);
        String[] ips = { "10.0.0.1", "::ffff:10.0.0.2", "2001:db8::1", "192.168.1.200", "fe80::1" };
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(ips.length)) {
            for (String ip : ips) {
                builder.appendBytesRef(new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip))));
            }
            int[] shards = shardsOf(partners(PartnerShards.KeyType.IP, routing).split(new Page(builder.build())), ips.length);
            for (int p = 0; p < ips.length; p++) {
                assertThat(ips[p], shards[p], equalTo(documentShard(routing, field, "\"" + ips[p] + "\"")));
            }
        }
    }

    /**
     * Null keys are left out, multi-valued keys join the first group, and groups list their rows in order.
     */
    public void testNullAndMultiValuedKeys() {
        IndexRouting routing = routing("long");
        long a = keyOnShard(routing, 1);
        long b = keyOnShard(routing, 3);
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(5)) {
            builder.appendLong(b);
            builder.appendNull();
            builder.beginPositionEntry().appendLong(a).appendLong(b).endPositionEntry();
            builder.appendLong(a);
            builder.appendLong(b);
            List<PartnerShards.Group> groups = partners(PartnerShards.KeyType.LONG, routing).split(new Page(builder.build()));
            assertThat(groups, hasSize(2));
            assertThat(groups.get(0).shard(), equalTo(1));
            assertThat(groups.get(0).positions(), equalTo(new int[] { 3 }));
            assertThat(groups.get(1).shard(), equalTo(3));
            assertThat(groups.get(1).positions(), equalTo(new int[] { 0, 2, 4 }));
        }
    }

    public void testSingleShardPageIsNotSplit() {
        IndexRouting routing = routing("long");
        long a = keyOnShard(routing, 2);
        try (LongBlock block = blockFactory.newConstantLongBlockWith(a, 10)) {
            List<PartnerShards.Group> groups = partners(PartnerShards.KeyType.LONG, routing).split(new Page(block));
            assertThat(groups, hasSize(1));
            assertThat(groups.get(0).shard(), equalTo(2));
            assertThat(groups.get(0).positions(), nullValue());
        }
    }

    /**
     * A page from an aligned left shard goes to the partner shard as a whole, without looking at the keys.
     */
    public void testAlignedPageIsNotHashed() {
        IndexRouting routing = routing("long");
        long onShard3 = keyOnShard(routing, 3);
        Page page = docPage(4, onShard3);
        try {
            PartnerShards aligned = new PartnerShards(1, PartnerShards.KeyType.LONG, routing, SHARDS, 0, shardIndex -> shardIndex);
            List<PartnerShards.Group> groups = aligned.split(page);
            assertThat(groups, hasSize(1));
            assertThat(groups.get(0).shard(), equalTo(4));
            assertThat(groups.get(0).positions(), nullValue());
            // A left shard that is not aligned falls back to the keys.
            PartnerShards notAligned = new PartnerShards(1, PartnerShards.KeyType.LONG, routing, SHARDS, 0, shardIndex -> -1);
            assertThat(notAligned.split(page).get(0).shard(), equalTo(3));
        } finally {
            page.releaseBlocks();
        }
    }

    private Page docPage(int shardIndex, long key) {
        int rows = 3;
        DocVector docs = new DocVector(
            AlwaysReferencedIndexedByShardId.INSTANCE,
            blockFactory.newConstantIntVector(shardIndex, rows),
            blockFactory.newConstantIntVector(0, rows),
            blockFactory.newIntArrayVector(new int[] { 0, 1, 2 }, rows),
            DocVector.config()
        );
        return new Page(docs.asBlock(), blockFactory.newConstantLongBlockWith(key, rows));
    }

    private static PartnerShards partners(PartnerShards.KeyType keyType, IndexRouting routing) {
        return new PartnerShards(0, keyType, routing, SHARDS, -1, null);
    }

    private static int[] shardsOf(List<PartnerShards.Group> groups, int positions) {
        int[] shards = new int[positions];
        for (PartnerShards.Group group : groups) {
            if (group.positions() == null) {
                Arrays.fill(shards, group.shard());
            } else {
                for (int p : group.positions()) {
                    shards[p] = group.shard();
                }
            }
        }
        return shards;
    }

    private static long keyOnShard(IndexRouting routing, int shard) {
        for (long key = 0;; key++) {
            String r = RoutingField.canonicalIntegral(key);
            if (routing.getShard(r, r) == shard) {
                return key;
            }
        }
    }

    /**
     * The shard of a document of the right index whose routing field {@code k} has {@code jsonValue}.
     */
    private static int documentShard(IndexRouting routing, RoutingField field, String jsonValue) {
        String value = field.extract(XContentType.JSON, new BytesArray("{\"k\": " + jsonValue + "}"));
        return routing.getShard("id", value);
    }

    private static IndexRouting routing(String type) {
        return IndexRouting.fromIndexMetadata(
            IndexMetadata.builder("right")
                .settings(indexSettings(IndexVersion.current(), SHARDS, 0).put(IndexSettings.ROUTING_FIELD.getKey(), "k"))
                .putMapping("{\"_doc\": {\"_routing\": {\"required\": true}, \"properties\": {\"k\": {\"type\": \"" + type + "\"}}}}")
                .build()
        );
    }
}
