/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * The page codec of a spilled run: the engine's block codecs for every block but the attached document references,
 * which a spilled page keeps attached to the node's shards since the rows come back to the same compute. A page read
 * back names the shard references it takes its documents' references from; a run that holds none is read without
 * them. Document references that came through an exchange are detached, carry their origins and go through the
 * engine's codec. Batch metadata is not carried: a batch marker is an empty page, which never spills.
 */
public final class SpillPages {

    private static final byte BLOCK = 0;
    private static final byte DOCUMENTS = 1;

    private SpillPages() {}

    public static void write(Page page, StreamOutput out) throws IOException {
        out.writeVInt(page.getPositionCount());
        out.writeVInt(page.getBlockCount());
        for (int b = 0; b < page.getBlockCount(); b++) {
            Block block = page.getBlock(b);
            if (block instanceof DocBlock documents && documents.asVector().origins() == null) {
                // attached: the slots mean something to this node only, and the page stays on it
                out.writeByte(DOCUMENTS);
                documents.asVector().writeLocal(out);
            } else {
                // a detached vector, one that came through an exchange, carries its origins and pins nothing
                out.writeByte(BLOCK);
                Block.writeTypedBlock(block, out);
            }
        }
    }

    /**
     * @param shardRefs the node's shard references for the document references the page may hold, or null when the run
     *                  was written by an operator that saw none
     */
    public static Page read(BlockStreamInput in, @Nullable IndexedByShardId<? extends RefCounted> shardRefs) throws IOException {
        int positionCount = in.readVInt();
        int blockCount = in.readVInt();
        Block[] blocks = new Block[blockCount];
        boolean success = false;
        try {
            for (int b = 0; b < blockCount; b++) {
                byte kind = in.readByte();
                blocks[b] = switch (kind) {
                    case BLOCK -> Block.readTypedBlock(in);
                    case DOCUMENTS -> {
                        if (shardRefs == null) {
                            throw new IllegalStateException(
                                "a spilled page references documents but the run was read without shard references"
                            );
                        }
                        yield DocVector.readLocal(in.blockFactory(), in, shardRefs).asBlock();
                    }
                    default -> throw new IllegalStateException("unknown spilled block kind [" + kind + "]");
                };
            }
            Page page = new Page(positionCount, blocks);
            success = true;
            return page;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }
}
