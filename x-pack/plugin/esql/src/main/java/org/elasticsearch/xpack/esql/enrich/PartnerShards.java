/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingField;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntUnaryOperator;

/**
 * For the co-located join: which shard of the right index holds the matches of each row of a left page. The right index is
 * routed by the join key ({@code index.routing_field}), so all documents with a key value are on the shard that the
 * value's canonical routing hashes to, and a row only needs a lookup on that one shard.
 * <p>
 * Two ways to find it:
 * <ul>
 *     <li><b>aligned</b>: the page was read from a left shard whose index routes by the same key, the same way, as the right
 *     index. Every key of the page then hashes to the right shard with the same number, and nothing needs hashing;</li>
 *     <li><b>routed</b>: otherwise each key is hashed like {@link IndexRouting} hashes the routing of a document of the right
 *     index, and the page is split by target shard.</li>
 * </ul>
 */
public final class PartnerShards {

    /**
     * How the join key is read from its block to make the routing value the right index uses.
     */
    public enum KeyType {
        KEYWORD,
        IP,
        INTEGER,
        LONG
    }

    /**
     * The rows of a page whose matches are on one right shard.
     *
     * @param positions the rows, ascending; {@code null} for every row of the page
     */
    public record Group(int shard, @Nullable int[] positions) {
        @Override
        public String toString() {
            return "Group[shard=" + shard + ", positions=" + (positions == null ? "all" : Arrays.toString(positions)) + "]";
        }
    }

    private static final int NO_MATCH = -1;
    private static final int MULTI_VALUED = -2;

    private final int keyChannel;
    private final KeyType keyType;
    private final IndexRouting rightRouting;
    private final int rightShards;
    private final int docChannel;
    @Nullable
    private final IntUnaryOperator alignedPartner;

    /**
     * @param keyChannel     channel of the join key in the left page
     * @param keyType        how to read the join key
     * @param rightRouting   routing of the right index
     * @param rightShards    number of shards of the right index
     * @param docChannel     channel of the left page's {@code _doc} column, or {@code -1} if it has none
     * @param alignedPartner maps the shard index of a left page (from its {@code _doc} column) to the right shard that holds
     *                       every key of that left shard, or {@code -1} if the left shard is not aligned with the right index;
     *                       {@code null} to always route
     */
    public PartnerShards(
        int keyChannel,
        KeyType keyType,
        IndexRouting rightRouting,
        int rightShards,
        int docChannel,
        @Nullable IntUnaryOperator alignedPartner
    ) {
        this.keyChannel = keyChannel;
        this.keyType = keyType;
        this.rightRouting = rightRouting;
        this.rightShards = rightShards;
        this.docChannel = docChannel;
        this.alignedPartner = alignedPartner;
    }

    /**
     * Splits the rows of a page by the right shard that holds their matches. Rows with a {@code null} key match nothing in an
     * inner join and are left out. Rows with a multi-valued key can't be routed to one shard; they join the first group,
     * whose lookup then reports them as it does for a lookup join (no match and a warning).
     */
    public List<Group> split(Page page) {
        int aligned = alignedShard(page);
        if (aligned >= 0) {
            return List.of(new Group(aligned, null));
        }
        Block keys = page.getBlock(keyChannel);
        int positionCount = page.getPositionCount();
        int[] targets = new int[positionCount];
        int[] counts = new int[rightShards];
        int multiValuedTarget = -1;
        int multiValued = 0;
        BytesRef scratch = new BytesRef();
        for (int p = 0; p < positionCount; p++) {
            int valueCount = keys.getValueCount(p);
            if (valueCount == 0) {
                targets[p] = NO_MATCH;
            } else if (valueCount > 1) {
                targets[p] = MULTI_VALUED;
                multiValued++;
            } else {
                String routing = routing(keys, keys.getFirstValueIndex(p), scratch);
                int shard = rightRouting.getShard(routing, routing);
                targets[p] = shard;
                counts[shard]++;
                if (multiValuedTarget < 0) {
                    multiValuedTarget = shard;
                }
            }
        }
        if (multiValued > 0) {
            if (multiValuedTarget < 0) {
                multiValuedTarget = 0;
            }
            counts[multiValuedTarget] += multiValued;
            for (int p = 0; p < positionCount; p++) {
                if (targets[p] == MULTI_VALUED) {
                    targets[p] = multiValuedTarget;
                }
            }
        }
        List<Group> groups = new ArrayList<>();
        int[][] positions = new int[rightShards][];
        for (int shard = 0; shard < rightShards; shard++) {
            if (counts[shard] == positionCount) {
                return List.of(new Group(shard, null));
            }
            if (counts[shard] > 0) {
                positions[shard] = new int[counts[shard]];
                groups.add(new Group(shard, positions[shard]));
            }
        }
        int[] filled = new int[rightShards];
        for (int p = 0; p < positionCount; p++) {
            int shard = targets[p];
            if (shard >= 0) {
                positions[shard][filled[shard]++] = p;
            }
        }
        return groups;
    }

    /**
     * The right shard of an aligned page, or {@code -1}.
     */
    private int alignedShard(Page page) {
        if (alignedPartner == null || docChannel < 0) {
            return -1;
        }
        IntVector shards = ((DocBlock) page.getBlock(docChannel)).asVector().shards();
        int first = shards.getInt(0);
        for (int i = 1; i < shards.getPositionCount(); i++) {
            if (shards.getInt(i) != first) {
                return -1;
            }
        }
        return alignedPartner.applyAsInt(first);
    }

    private String routing(Block keys, int valueIndex, BytesRef scratch) {
        return switch (keyType) {
            case KEYWORD -> ((BytesRefBlock) keys).getBytesRef(valueIndex, scratch).utf8ToString();
            case IP -> {
                BytesRef ip = ((BytesRefBlock) keys).getBytesRef(valueIndex, scratch);
                yield RoutingField.canonicalIp(Arrays.copyOfRange(ip.bytes, ip.offset, ip.offset + ip.length));
            }
            case INTEGER -> RoutingField.canonicalIntegral(((IntBlock) keys).getInt(valueIndex));
            case LONG -> RoutingField.canonicalIntegral(((LongBlock) keys).getLong(valueIndex));
        };
    }

    @Override
    public String toString() {
        return "PartnerShards[key="
            + keyChannel
            + " "
            + keyType
            + ", shards="
            + rightShards
            + ", aligned="
            + (alignedPartner != null)
            + "]";
    }
}
