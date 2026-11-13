/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;

import java.util.function.Function;

/**
 * Operator for the {@code WRITE} phase of an {@code INTO} command.
 * Aligns incoming pages to the destination's column order, delegates them to an
 * {@link IntoPageWriter}, and emits one {@code [total, created]} row once all
 * writes have settled.
 * <p>
 * In explain mode ({@link #explainOnly}), all input pages are discarded and no indexing occurs.
 * The operator always emits {@code [0, 0]}.
 */
public final class IntoOperator implements Operator {

    public static final class Factory implements OperatorFactory {
        @Nullable
        private final IntoPageWriter.Factory sink;
        private final Function<Page, Page> alignment;
        private final BlockFactory blockFactory;
        private final boolean explainOnly;

        public static Factory forWrite(IntoPageWriter.Factory sink, Function<Page, Page> alignment, BlockFactory blockFactory) {
            return new Factory(sink, alignment, blockFactory, false);
        }

        public static Factory forExplain(BlockFactory blockFactory) {
            return new Factory(null, Function.identity(), blockFactory, true);
        }

        private Factory(
            @Nullable IntoPageWriter.Factory sink,
            Function<Page, Page> alignment,
            BlockFactory blockFactory,
            boolean explainOnly
        ) {
            this.sink = sink;
            this.alignment = alignment;
            this.blockFactory = blockFactory;
            this.explainOnly = explainOnly;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            if (explainOnly) {
                return new IntoOperator(null, Function.identity(), blockFactory, true);
            }
            return new IntoOperator(sink.create(driverContext), alignment, blockFactory, false);
        }

        @Override
        public String describe() {
            return "IntoOperator" + (explainOnly ? "[explain]" : "[destination=" + sink.destination() + "]");
        }
    }

    @Nullable
    private final IntoPageWriter writer;
    private final Function<Page, Page> alignment;
    private final BlockFactory blockFactory;
    private final boolean explainOnly;

    private boolean inputFinished;
    private boolean outputEmitted;

    IntoOperator(@Nullable IntoPageWriter writer, Function<Page, Page> alignment, BlockFactory blockFactory, boolean explainOnly) {
        this.writer = writer;
        this.alignment = alignment;
        this.blockFactory = blockFactory;
        this.explainOnly = explainOnly;
    }

    @Override
    public boolean needsInput() {
        return inputFinished == false && isBlocked().listener().isDone();
    }

    @Override
    public void addInput(Page page) {
        if (explainOnly) {
            page.releaseBlocks();
        } else {
            writer.consume(alignment.apply(page));
        }
    }

    @Override
    public void finish() {
        inputFinished = true;
        if (explainOnly == false) {
            writer.finish();
        }
    }

    @Override
    public boolean isFinished() {
        return outputEmitted;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        if (explainOnly) {
            return inputFinished && outputEmitted == false;
        }
        return inputFinished && writer.isFinished() && outputEmitted == false;
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (explainOnly) {
            return NOT_BLOCKED;
        }
        return writer.isBlocked();
    }

    @Override
    public Page getOutput() {
        if (explainOnly) {
            if (inputFinished == false) {
                return null;
            }
            outputEmitted = true;
            return new IntoStats(0L, 0L).toPage(blockFactory);
        }
        if (inputFinished == false || writer.isFinished() == false) {
            return null;
        }
        ExceptionsHelper.reThrowIfNotNull(writer.failure());
        outputEmitted = true;
        return writer.stats().toPage(blockFactory);
    }

    @Override
    public void close() {}
}
