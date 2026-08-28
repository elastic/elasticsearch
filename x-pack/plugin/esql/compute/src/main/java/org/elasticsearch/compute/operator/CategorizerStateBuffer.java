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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;

import java.util.ArrayDeque;

/**
 * Shared buffering logic for {@link CategorizeGroupingOperator} (INITIAL mode) and
 * {@link CategorizeGroupingMergeOperator} (INTERMEDIATE mode).
 *
 * <p>When {@code emitState} is {@code false} (SINGLE and FINAL phases), every method
 * is a pass-through to the inner operator and no buffering occurs.
 *
 * <p>When {@code emitState} is {@code true} (INITIAL and INTERMEDIATE phases), this object
 * withholds every output page that the inner operator produces until the owning operator has received
 * all input (i.e. until its {@link #finish()} is called). Once input is exhausted the
 * categorizer model is final. On the first call to {@link #getOutput()} after {@code finish()},
 * the model is serialized once via {@link CategorizerStateCodec#serialize} and a constant
 * {@link BytesRefBlock} carrying that state is appended to every page before it is returned.
 *
 * <p>The buffer borrows {@code inner} and {@code model}; the owning operator remains responsible
 * for closing them. This object owns only the queued {@link Page} instances, which are released
 * in {@link #close()}.
 *
 * <p>The eager drain in {@link #drainInner()} is required because
 * {@link GroupedLimitOperator#needsInput()} returns {@code false} whenever a page is pending
 * in its {@code lastOutput} field. Without the eager drain, the outer operator's
 * {@code needsInput()} would also go {@code false} while {@code getOutput()} still returns
 * {@code null} (since buffered output is withheld pre-{@code finish}), permanently stalling
 * the driver pipeline.
 */
final class CategorizerStateBuffer implements Releasable {

    private final BlockFactory blockFactory;
    private final Operator inner;
    private final TokenListCategorizer.CloseableTokenListCategorizer model;
    private final boolean emitState;

    /** Pages held back until input is exhausted. Only used when {@code emitState == true}. */
    private final ArrayDeque<Page> buffered = new ArrayDeque<>();

    /** Set to {@code true} once {@link #finish()} has been called. */
    private boolean finished;

    /**
     * Serialized categorizer state, computed lazily on the first {@link #getOutput()} call after
     * {@link #finish()}, then reused for every subsequent page. {@code null} until then.
     */
    private BytesRef finalState;

    /**
     * @param blockFactory used to allocate the constant state block appended to each output page
     * @param inner        the inner grouping operator; borrowed — caller closes it
     * @param model        the local categorizer; borrowed — caller closes it
     * @param emitState    {@code true} for INITIAL and INTERMEDIATE phases; {@code false} for SINGLE/FINAL
     */
    CategorizerStateBuffer(
        BlockFactory blockFactory,
        Operator inner,
        TokenListCategorizer.CloseableTokenListCategorizer model,
        boolean emitState
    ) {
        this.blockFactory = blockFactory;
        this.inner = inner;
        this.model = model;
        this.emitState = emitState;
    }

    /**
     * Drains any pages the inner operator has ready and, when buffering is active, holds them back
     * until {@link #finish()} is called.
     *
     * <p>Must be called by the owning operator immediately after every {@link Operator#addInput}
     * delegation to the inner operator.
     */
    void drainInner() {
        if (emitState == false) {
            return;
        }
        Page p;
        while ((p = inner.getOutput()) != null) {
            buffered.add(p);
        }
    }

    /**
     * Marks input as exhausted. Must be called by the owning operator in its own
     * {@link Operator#finish()} <em>after</em> delegating {@code finish()} to the inner operator,
     * so that operators like {@link org.elasticsearch.compute.operator.topn.GroupedTopNOperator}
     * have materialised their output iterator before the buffer begins serving output.
     */
    void finish() {
        finished = true;
    }

    /**
     * Returns {@code true} once all buffered pages and any remaining inner-operator output have
     * been consumed.
     */
    boolean isFinished() {
        if (emitState == false) {
            return inner.isFinished();
        }
        return finished && buffered.isEmpty() && inner.isFinished();
    }

    /**
     * Returns {@code true} when buffered or inner-operator pages are ready to be served without
     * additional input. Pre-{@code finish} this is always {@code false} because withheld pages
     * are not yet available to callers.
     */
    boolean canProduceMoreDataWithoutExtraInput() {
        if (emitState == false) {
            return inner.canProduceMoreDataWithoutExtraInput();
        }
        return finished && (buffered.isEmpty() == false || inner.canProduceMoreDataWithoutExtraInput());
    }

    /**
     * Returns the next output page, or {@code null} if none is available yet.
     *
     * <p>When {@code emitState} is {@code false}, delegates directly to the inner operator.
     * When {@code emitState} is {@code true}, returns {@code null} until after {@link #finish()}
     * is called; afterwards drains the buffer (then the inner operator) and appends the final
     * categorizer state as a constant {@link BytesRefBlock} to each page.
     */
    Page getOutput() {
        if (emitState == false) {
            return inner.getOutput();
        }
        if (finished == false) {
            return null;
        }

        // Lazily compute the final state once.
        if (finalState == null) {
            finalState = CategorizerStateCodec.serialize(model);
        }

        Page p = buffered.isEmpty() ? inner.getOutput() : buffered.poll();
        if (p == null) {
            return null;
        }
        BytesRefBlock stateBlock = null;
        boolean success = false;
        try {
            stateBlock = blockFactory.newConstantBytesRefBlockWith(finalState, p.getPositionCount());
            Page result = p.appendBlock(stateBlock);
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(stateBlock);
                p.releaseBlocks();
            }
        }
    }

    @Override
    public void close() {
        // The inner operator and the model are closed by the owning operator.
        Releasables.closeExpectNoException(() -> Releasables.close(buffered));
    }
}
