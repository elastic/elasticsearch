/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.core.Releasables;

import java.util.function.IntFunction;

/**
 * Detaches the documents of every page before the page leaves the node, see {@link DocVector#detach}. A {@link DocBlock}
 * whose rows are still on this node gets the origins of its shard slots, so its rows can cross the wire and come back to
 * the node that loads them. The other blocks pass through.
 * <p>
 * The pages of the coordinator's own node are never serialized, so detaching can't wait for {@link DocBlock#writeTo}.
 * It runs on the thread that adds the page, a driver thread, before the node reports which readers its rows came from.
 */
public final class DetachingExchangeSink implements ExchangeSink {
    private final ExchangeSink delegate;
    private final IntFunction<DocOrigin> originOfSlot;

    /**
     * @param originOfSlot the origin of a shard slot of this node. Only slots that rows reference are asked for.
     */
    public DetachingExchangeSink(ExchangeSink delegate, IntFunction<DocOrigin> originOfSlot) {
        this.delegate = delegate;
        this.originOfSlot = originOfSlot;
    }

    /**
     * Takes ownership of {@code page}, also when detaching fails.
     */
    @Override
    public void addPage(Page page) {
        delegate.addPage(detach(page, originOfSlot));
    }

    static Page detach(Page page, IntFunction<DocOrigin> originOfSlot) {
        if (hasRowsOfThisNode(page) == false) {
            return page;
        }
        Block[] blocks = new Block[page.getBlockCount()];
        boolean success = false;
        try {
            for (int b = 0; b < blocks.length; b++) {
                Block block = page.getBlock(b);
                if (block instanceof DocBlock doc && doc.asVector().origins() == null) {
                    blocks[b] = doc.asVector().detach(originOfSlot).asBlock();
                } else {
                    block.incRef();
                    blocks[b] = block;
                }
            }
            Page detached = page.batchMetadata() == null
                ? new Page(page.getPositionCount(), blocks)
                : new Page(page.batchMetadata(), blocks);
            success = true;
            return detached;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
            // the detached page holds its own references, and releasing the doc blocks releases their shards
            page.releaseBlocks();
        }
    }

    private static boolean hasRowsOfThisNode(Page page) {
        for (int b = 0; b < page.getBlockCount(); b++) {
            if (page.getBlock(b) instanceof DocBlock doc && doc.asVector().origins() == null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void finish() {
        delegate.finish();
    }

    @Override
    public boolean isFinished() {
        return delegate.isFinished();
    }

    @Override
    public void addCompletionListener(ActionListener<Void> listener) {
        delegate.addCompletionListener(listener);
    }

    @Override
    public IsBlockedResult waitForWriting() {
        return delegate.waitForWriting();
    }
}
