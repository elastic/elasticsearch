/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold.compute;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;

/**
 * Emits the live documents of a list of leaves as {@link DocVector} pages, one leaf at a time in ascending doc order,
 * so that the values reader can use its single-segment fast path. This stands in for {@code LuceneSourceOperator},
 * which needs a full shard context (searcher, stats, queries); at merge time we only have bare readers.
 */
final class SegmentDocSourceOperator extends SourceOperator {
    private final BlockFactory blockFactory;
    private final List<LeafReaderContext> leaves;
    private final int maxPageSize;
    private int leaf;
    private int doc;

    SegmentDocSourceOperator(BlockFactory blockFactory, List<LeafReaderContext> leaves, int maxPageSize) {
        this.blockFactory = blockFactory;
        this.leaves = leaves;
        this.maxPageSize = maxPageSize;
    }

    @Override
    public Page getOutput() {
        while (leaf < leaves.size()) {
            LeafReaderContext ctx = leaves.get(leaf);
            LeafReader reader = ctx.reader();
            Bits live = reader.getLiveDocs();
            int maxDoc = reader.maxDoc();
            int[] docs = new int[Math.min(maxPageSize, maxDoc - doc)];
            int n = 0;
            while (doc < maxDoc && n < docs.length) {
                if (live == null || live.get(doc)) {
                    docs[n++] = doc;
                }
                doc++;
            }
            if (doc >= maxDoc) {
                leaf++;
                doc = 0;
            }
            if (n == 0) {
                continue;
            }
            return page(ctx.ord, n < docs.length ? Arrays.copyOf(docs, n) : docs, n);
        }
        return null;
    }

    private Page page(int segment, int[] docs, int n) {
        IntVector shards = null;
        IntVector segments = null;
        IntVector docIds = null;
        boolean success = false;
        try {
            shards = blockFactory.newConstantIntVector(0, n);
            segments = blockFactory.newConstantIntVector(segment, n);
            docIds = blockFactory.newIntArrayVector(docs, n);
            DocVector vector = new DocVector(
                AlwaysReferencedIndexedByShardId.INSTANCE,
                shards,
                segments,
                docIds,
                DocVector.config().singleSegmentNonDecreasing(true)
            );
            success = true;
            return new Page(vector.asBlock());
        } finally {
            if (success == false) {
                Releasables.close(shards, segments, docIds);
            }
        }
    }

    @Override
    public void finish() {
        leaf = leaves.size();
    }

    @Override
    public boolean isFinished() {
        return leaf >= leaves.size();
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "SegmentDocSourceOperator[leaves=" + leaves.size() + "]";
    }
}
