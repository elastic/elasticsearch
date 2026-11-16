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

import java.util.function.Function;

/**
 * Operator for the {@code WRITE} phase of an {@code INTO} command.
 * Aligns incoming pages to the destination's column order, delegates them to an
 * {@link IntoPageWriter}, and emits one {@code [total, created]} row once all
 * writes have settled.
 */
public final class IntoOperator implements Operator {
    public record Factory(IntoPageWriter.Factory sink, Function<Page, Page> alignment) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new IntoOperator(sink.create(driverContext), alignment, driverContext.blockFactory());
        }

        @Override
        public String describe() {
            return "IntoOperator[destination=" + sink.destination() + "]";
        }
    }

    private final IntoPageWriter writer;
    private final Function<Page, Page> alignment;
    private final BlockFactory blockFactory;
    private boolean inputFinished;
    private boolean outputEmitted;

    private IntoOperator(IntoPageWriter writer, Function<Page, Page> alignment, BlockFactory blockFactory) {
        this.writer = writer;
        this.alignment = alignment;
        this.blockFactory = blockFactory;
    }

    @Override
    public boolean needsInput() {
        return inputFinished == false && writer.isBlocked().listener().isDone();
    }

    @Override
    public void addInput(Page page) {
        Exception failure = writer.failure();
        if (failure != null) {
            page.releaseBlocks();
            throw ExceptionsHelper.convertToRuntime(failure);
        }
        writer.consume(alignment.apply(page));
    }

    @Override
    public void finish() {
        inputFinished = true;
        writer.finish();
    }

    @Override
    public boolean isFinished() {
        return outputEmitted;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return writer.isFinished() && outputEmitted == false;
    }

    @Override
    public IsBlockedResult isBlocked() {
        return writer.isBlocked();
    }

    @Override
    public Page getOutput() {
        if (writer.isFinished() == false) {
            return null;
        }
        Exception failure = writer.failure();
        if (failure != null) {
            throw ExceptionsHelper.convertToRuntime(failure);
        }
        outputEmitted = true;
        return writer.stats().toPage(blockFactory);
    }

    @Override
    public void close() {}
}
