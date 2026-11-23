/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups the documents of a cut into one fetch request per shard, and remembers which response row each row of the cut
 * reads.
 * <p>
 * Each shard gets a request of its own, like in the fetch phase of {@code _search}. A node runs each request on a driver
 * of its own, so it loads the documents of its shards in parallel, not one shard after the other. A request asks for the
 * documents of its shard sorted by segment and doc, the order the node loads them fastest and returns their rows in. A
 * document that several rows reference is asked for once. The rows of the cut find their values by position in the
 * responses, read in request order, without a position column on the wire.
 * <p>
 * Each reader context appears in one request only, so every request asks its node to free its context once it answered.
 */
final class FetchBatchPlanner {
    private FetchBatchPlanner() {}

    /**
     * The documents of one shard, which one request fetches from {@code node}.
     */
    record ShardBatch(FetchNode node, FetchRequest.ShardDocs docs) {}

    /**
     * The requests for the rows of a cut.
     *
     * @param shards       the documents of each shard, in the order the shards first appeared in the cut
     * @param responseRows for each row of the cut, the row that holds its values when the responses of {@code shards}
     *                     are read one after the other
     * @param deduplicated whether several rows read the same response row
     */
    record Batch(List<ShardBatch> shards, int[] responseRows, boolean deduplicated) {}

    /**
     * @param pages      the rows of the cut
     * @param docChannel the channel of the documents, which left the nodes that read them
     */
    static Batch plan(List<Page> pages, int docChannel) {
        int rows = 0;
        for (Page page : pages) {
            rows += page.getPositionCount();
        }
        Map<DocOrigin, ShardAccumulator> shards = new LinkedHashMap<>();
        ShardAccumulator[] shardOfRow = new ShardAccumulator[rows];
        long[] docOfRow = new long[rows];
        int row = 0;
        for (Page page : pages) {
            Block block = page.getBlock(docChannel);
            DocOrigins origins = block instanceof DocBlock docBlock ? docBlock.asVector().origins() : null;
            if (origins == null) {
                throw new IllegalArgumentException(
                    "expected documents that left their node in channel [" + docChannel + "] but got " + block
                );
            }
            DocVector documents = ((DocBlock) block).asVector();
            IntVector originOrdinals = documents.shards();
            IntVector segments = documents.segments();
            IntVector docs = documents.docs();
            // each page has its own origins, usually a handful for many rows, so the shard is looked up once per origin
            ShardAccumulator[] shardOfOrigin = new ShardAccumulator[origins.size()];
            for (int p = 0; p < page.getPositionCount(); p++) {
                int ordinal = originOrdinals.getInt(p);
                ShardAccumulator shard = shardOfOrigin[ordinal];
                if (shard == null) {
                    // an origin names exactly one reader context, of one shard on one node
                    shard = shards.computeIfAbsent(origins.get(ordinal), ShardAccumulator::new);
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

        List<ShardBatch> batches = new ArrayList<>(shards.size());
        boolean deduplicated = false;
        int responseRow = 0;
        for (ShardAccumulator shard : shards.values()) {
            FetchNode node = new FetchNode(shard.origin.clusterAlias(), shard.origin.nodeId());
            batches.add(new ShardBatch(node, shard.sort(responseRow)));
            responseRow += shard.distinctDocs;
            deduplicated |= shard.distinctDocs < shard.rows;
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

    private static final class ShardAccumulator {
        private final DocOrigin origin;
        private int rows;
        /** The packed document of each row, then sorted, with the distinct documents first. */
        private long[] docs;
        private int added;
        private int distinctDocs;
        private int firstResponseRow;

        ShardAccumulator(DocOrigin origin) {
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
            // no other request reads the context, so the node frees it once it answered
            return new FetchRequest.ShardDocs(origin.shardId(), origin.contextId(), true, segments, docIds);
        }

        int responseRowOf(long segmentAndDoc) {
            return firstResponseRow + Arrays.binarySearch(docs, 0, distinctDocs, segmentAndDoc);
        }
    }
}
