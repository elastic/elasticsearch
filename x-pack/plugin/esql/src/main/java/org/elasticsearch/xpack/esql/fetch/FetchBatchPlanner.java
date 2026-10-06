/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocRefBlock;
import org.elasticsearch.compute.data.DocRefOrigin;
import org.elasticsearch.compute.data.DocRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups the document references of a cut into one fetch request per node, and remembers which response row each row of
 * the cut reads.
 * <p>
 * A node gets the documents of each of its shards sorted by segment and doc, the order it loads them fastest and
 * returns their rows in. A document that several rows reference is asked for once. The nodes answer with their rows in
 * request order, so the rows of the cut find their values by position, without a position column on the wire.
 */
final class FetchBatchPlanner {
    private FetchBatchPlanner() {}

    /**
     * The documents of one node.
     *
     * @param shards the documents of each shard, in the order the shards first appeared in the cut
     */
    record NodeBatch(String clusterAlias, String nodeId, List<FetchRequest.ShardDocs> shards) {
        int docCount() {
            int docs = 0;
            for (FetchRequest.ShardDocs shard : shards) {
                docs += shard.docCount();
            }
            return docs;
        }

        List<ShardSearchContextId> contextIds() {
            return shards.stream().map(FetchRequest.ShardDocs::contextId).toList();
        }
    }

    /**
     * The requests for the rows of a cut.
     *
     * @param nodes        the documents of each node, in the order the nodes first appeared in the cut
     * @param responseRows for each row of the cut, the row that holds its values when the responses of {@code nodes}
     *                     are read one after the other
     * @param deduplicated whether several rows read the same response row
     */
    record Batch(List<NodeBatch> nodes, int[] responseRows, boolean deduplicated) {}

    /**
     * @param pages         the rows of the cut
     * @param docRefChannel the channel of the document references
     */
    static Batch plan(List<Page> pages, int docRefChannel) {
        int rows = 0;
        for (Page page : pages) {
            rows += page.getPositionCount();
        }
        Map<NodeKey, NodeAccumulator> nodes = new LinkedHashMap<>();
        ShardAccumulator[] shardOfRow = new ShardAccumulator[rows];
        long[] docOfRow = new long[rows];
        int row = 0;
        for (Page page : pages) {
            Block block = page.getBlock(docRefChannel);
            if (block instanceof DocRefBlock == false) {
                throw new IllegalArgumentException("expected document references in channel [" + docRefChannel + "] but got " + block);
            }
            DocRefVector references = ((DocRefBlock) block).asVector();
            IntVector originOrdinals = references.originOrdinals();
            IntVector segments = references.segments();
            IntVector docs = references.docs();
            // each page has its own origins, usually a handful for many rows, so the shard is looked up once per origin
            ShardAccumulator[] shardOfOrigin = new ShardAccumulator[references.origins().size()];
            for (int p = 0; p < page.getPositionCount(); p++) {
                int ordinal = originOrdinals.getInt(p);
                ShardAccumulator shard = shardOfOrigin[ordinal];
                if (shard == null) {
                    DocRefOrigin origin = references.origins().get(ordinal);
                    NodeAccumulator node = nodes.computeIfAbsent(new NodeKey(origin.clusterAlias(), origin.nodeId()), NodeAccumulator::new);
                    shard = node.shards.computeIfAbsent(origin.contextId(), id -> new ShardAccumulator(origin));
                    shardOfOrigin[ordinal] = shard;
                }
                shard.rows++;
                shardOfRow[row] = shard;
                docOfRow[row] = segmentAndDoc(segments.getInt(p), docs.getInt(p));
                row++;
            }
        }
        for (int r = 0; r < rows; r++) {
            shardOfRow[r].add(docOfRow[r]);
        }

        List<NodeBatch> batches = new ArrayList<>(nodes.size());
        boolean deduplicated = false;
        int responseRow = 0;
        for (NodeAccumulator node : nodes.values()) {
            List<FetchRequest.ShardDocs> shards = new ArrayList<>(node.shards.size());
            for (ShardAccumulator shard : node.shards.values()) {
                shards.add(shard.sort(responseRow));
                responseRow += shard.distinctDocs;
                deduplicated |= shard.distinctDocs < shard.rows;
            }
            batches.add(new NodeBatch(node.key.clusterAlias(), node.key.nodeId(), shards));
        }
        int[] responseRows = new int[rows];
        for (int r = 0; r < rows; r++) {
            responseRows[r] = shardOfRow[r].responseRowOf(docOfRow[r]);
        }
        return new Batch(batches, responseRows, deduplicated);
    }

    /**
     * Segments and docs are never negative, so the packed values sort by segment and then by doc.
     */
    private static long segmentAndDoc(int segment, int doc) {
        return ((long) segment << 32) | (doc & 0xFFFFFFFFL);
    }

    private record NodeKey(String clusterAlias, String nodeId) {}

    private static final class NodeAccumulator {
        private final NodeKey key;
        /** By context id, which names exactly one shard on one searcher. */
        private final Map<ShardSearchContextId, ShardAccumulator> shards = new LinkedHashMap<>();

        NodeAccumulator(NodeKey key) {
            this.key = key;
        }
    }

    private static final class ShardAccumulator {
        private final DocRefOrigin origin;
        private int rows;
        /** The packed document of each row, then sorted, with the distinct documents first. */
        private long[] docs;
        private int added;
        private int distinctDocs;
        private int firstResponseRow;

        ShardAccumulator(DocRefOrigin origin) {
            this.origin = origin;
        }

        void add(long segmentAndDoc) {
            if (docs == null) {
                docs = new long[rows];
            }
            docs[added++] = segmentAndDoc;
        }

        /**
         * Sorts the documents by segment and doc, the order the node returns their rows in, and drops the duplicates.
         *
         * @param firstResponseRow the response row of the first document of this shard
         */
        FetchRequest.ShardDocs sort(int firstResponseRow) {
            this.firstResponseRow = firstResponseRow;
            Arrays.sort(docs);
            distinctDocs = 0;
            for (int i = 0; i < docs.length; i++) {
                if (i == 0 || docs[i] != docs[distinctDocs - 1]) {
                    docs[distinctDocs++] = docs[i];
                }
            }
            int[] segments = new int[distinctDocs];
            int[] docIds = new int[distinctDocs];
            for (int i = 0; i < distinctDocs; i++) {
                segments[i] = (int) (docs[i] >>> 32);
                docIds[i] = (int) docs[i];
            }
            return new FetchRequest.ShardDocs(origin.shardId(), origin.contextId(), segments, docIds);
        }

        int responseRowOf(long segmentAndDoc) {
            return firstResponseRow + Arrays.binarySearch(docs, 0, distinctDocs, segmentAndDoc);
        }
    }
}
