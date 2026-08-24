/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Buffers all pages from upstream (which already have a local category-ID channel appended by
 * {@link CategorizeEvalOperator}), then at flush-time appends a constant
 * {@link BytesRefBlock} carrying the serialized {@link
 * org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer} state to every buffered
 * page.
 *
 * <p>Used in the INITIAL phase of distributed {@code LIMIT BY CATEGORIZE}: data nodes keep
 * N rows per local category, then attach the full categorizer model so the coordinator can
 * merge shard models and remap category IDs before applying the global limit.
 *
 * <p>The buffer is bounded because upstream {@link GroupedLimitOperator} already limits to
 * {@code N × numCategories} rows.
 */
public class CategorizeStateEmitOperator implements Operator {

    public static final class Factory implements Operator.OperatorFactory {
        /**
         * Single-element array written by {@link CategorizeEvalOperator.Factory#get} so that
         * this operator can read the categorizer state after all input has been processed.
         */
        private final CategorizeEvalOperator[] categorizeHolder;

        public Factory(CategorizeEvalOperator[] categorizeHolder) {
            this.categorizeHolder = categorizeHolder;
        }

        @Override
        public CategorizeStateEmitOperator get(DriverContext driverContext) {
            return new CategorizeStateEmitOperator(categorizeHolder, driverContext.blockFactory());
        }

        @Override
        public String describe() {
            return "CategorizeStateEmitOperator";
        }
    }

    private final CategorizeEvalOperator[] categorizeHolder;
    private final BlockFactory blockFactory;

    /** Pages buffered after GroupedLimitOperator filtering; emitted with state attached. */
    private final List<Page> buffered = new ArrayList<>();

    private boolean finished = false;
    private boolean emitting = false;
    private int nextEmitIndex = 0;
    private BytesRef serializedState = null;

    private CategorizeStateEmitOperator(CategorizeEvalOperator[] categorizeHolder, BlockFactory blockFactory) {
        this.categorizeHolder = categorizeHolder;
        this.blockFactory = blockFactory;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        assert finished == false : "addInput called after finish";
        buffered.add(page);
    }

    @Override
    public void finish() {
        if (finished == false) {
            finished = true;
            // Serialize the categorizer state once all input is in.
            CategorizeEvalOperator categorize = categorizeHolder[0];
            serializedState = categorize != null ? categorize.serializeCategorizer() : emptyState();
            emitting = true;
        }
    }

    @Override
    public boolean isFinished() {
        return emitting && nextEmitIndex >= buffered.size();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return emitting && nextEmitIndex < buffered.size();
    }

    @Override
    public Page getOutput() {
        if (emitting == false || nextEmitIndex >= buffered.size()) {
            return null;
        }
        Page bufferedPage = buffered.get(nextEmitIndex++);
        int positionCount = bufferedPage.getPositionCount();
        BytesRefBlock stateBlock = blockFactory.newConstantBytesRefBlockWith(serializedState, positionCount);
        boolean success = false;
        try {
            Page result = bufferedPage.appendBlock(stateBlock);
            success = true;
            return result;
        } finally {
            if (success == false) {
                stateBlock.close();
            }
        }
    }

    /** Returns a minimal serialized state representing an empty categorizer (seenNull=false, 0 categories). */
    private static BytesRef emptyState() {
        byte[] bytes = new byte[2]; // writeBoolean(false) = 1 byte, writeVInt(0) = 1 byte
        bytes[0] = 0; // seenNull = false
        bytes[1] = 0; // VInt 0 = 0x00
        return new BytesRef(bytes);
    }

    public long ramBytesUsed() {
        long size = 0;
        for (Page p : buffered) {
            size += p.ramBytesUsedByBlocks();
        }
        return size;
    }

    @Override
    public void close() {
        for (int i = nextEmitIndex; i < buffered.size(); i++) {
            Releasables.closeExpectNoException(buffered.get(i)::releaseBlocks);
        }
    }

    @Override
    public String toString() {
        return "CategorizeStateEmitOperator[]";
    }
}
