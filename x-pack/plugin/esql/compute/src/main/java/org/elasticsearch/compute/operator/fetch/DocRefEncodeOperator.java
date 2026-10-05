/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fetch;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocRefBlock;
import org.elasticsearch.compute.data.DocRefOrigin;
import org.elasticsearch.compute.data.DocRefOrigins;
import org.elasticsearch.compute.data.DocRefVector;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Replaces the {@code _doc} column with a {@link DocRefBlock}, so the rows can leave the node and still name their
 * documents. The other columns pass through.
 * <p>
 * The operator releases the {@link DocBlock}, and with it the last reference this page held on its shards. The new
 * block shares the segment and doc vectors and holds no reference: the reader context named by the origin keeps the
 * reader. It handles every page itself, empty ones included, because a {@link DocBlock} can't be written to the wire.
 */
public final class DocRefEncodeOperator implements Operator {
    /**
     * @param origins the origin of each shard slot of the page, usually {@link ShardContext#origin()} mapped lazily over
     *                the shard contexts. Only slots that rows reference are read.
     */
    public record Factory(int docChannel, IndexedByShardId<DocRefOrigin> origins) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new DocRefEncodeOperator(driverContext.blockFactory(), docChannel, origins);
        }

        @Override
        public String describe() {
            return "DocRefEncodeOperator[docChannel=" + docChannel + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final int docChannel;
    private final IndexedByShardId<DocRefOrigin> origins;

    private Page output;
    private boolean finished;

    private long processNanos;
    private int pagesProcessed;
    private long rowsReceived;
    private long rowsEmitted;

    public DocRefEncodeOperator(BlockFactory blockFactory, int docChannel, IndexedByShardId<DocRefOrigin> origins) {
        this.blockFactory = blockFactory;
        this.docChannel = docChannel;
        this.origins = origins;
    }

    @Override
    public boolean needsInput() {
        return output == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        long start = System.nanoTime();
        try {
            output = encode(page);
        } finally {
            processNanos += System.nanoTime() - start;
        }
        pagesProcessed++;
        rowsReceived += page.getPositionCount();
        rowsEmitted += output.getPositionCount();
    }

    private Page encode(Page page) {
        if (page.getBlockCount() == 0) {
            // a batch marker has no rows and no columns to convert
            return page;
        }
        try {
            Block block = page.getBlock(docChannel);
            if (block instanceof DocBlock == false) {
                throw new IllegalStateException("expected _doc at channel [" + docChannel + "] but got [" + block.elementType() + "]");
            }
            // the only step that can fail, so nothing else is referenced yet when it does
            DocRefBlock refs = encode(((DocBlock) block).asVector());
            Block[] blocks = new Block[page.getBlockCount()];
            for (int b = 0; b < blocks.length; b++) {
                if (b == docChannel) {
                    blocks[b] = refs;
                } else {
                    blocks[b] = page.getBlock(b);
                    blocks[b].incRef();
                }
            }
            return page.batchMetadata() == null ? new Page(page.getPositionCount(), blocks) : new Page(page.batchMetadata(), blocks);
        } finally {
            page.releaseBlocks();
        }
    }

    private DocRefBlock encode(DocVector doc) {
        int positions = doc.getPositionCount();
        IntVector shards = doc.shards();
        // The distinct shards in the order rows first reference them. A TopN builds the shard vector as an array even when
        // every row comes from one shard, so the check runs on the rows and not on isConstant.
        List<DocRefOrigin> referenced = new ArrayList<>();
        int[] ordinalOfSlot = null;
        if (positions > 0) {
            if (shards.isConstant()) {
                referenced.add(origins.get(shards.getInt(0)));
            } else {
                ordinalOfSlot = new int[shards.max() + 1];
                Arrays.fill(ordinalOfSlot, -1);
                for (int p = 0; p < positions; p++) {
                    int slot = shards.getInt(p);
                    if (ordinalOfSlot[slot] < 0) {
                        ordinalOfSlot[slot] = referenced.size();
                        referenced.add(origins.get(slot));
                    }
                }
            }
        }
        IntVector ordinals = null;
        IntVector segments = null;
        IntVector docs = null;
        DocRefBlock result = null;
        try {
            if (referenced.size() <= 1) {
                ordinals = blockFactory.newConstantIntVector(0, positions);
            } else {
                try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(positions)) {
                    for (int p = 0; p < positions; p++) {
                        builder.appendInt(ordinalOfSlot[shards.getInt(p)]);
                    }
                    ordinals = builder.build();
                }
            }
            segments = doc.segments();
            segments.incRef();
            docs = doc.docs();
            docs.incRef();
            result = new DocRefVector(DocRefOrigins.of(referenced), ordinals, segments, docs, doc.mayContainDuplicates()).asBlock();
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(ordinals, segments, docs);
            }
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && output == null;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return output != null;
    }

    @Override
    public Page getOutput() {
        Page result = output;
        output = null;
        return result;
    }

    @Override
    public Status status() {
        return new AbstractPageMappingOperator.Status(processNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }

    @Override
    public void close() {
        if (output != null) {
            output.releaseBlocks();
            output = null;
        }
    }

    @Override
    public String toString() {
        return "DocRefEncodeOperator[docChannel=" + docChannel + "]";
    }
}
