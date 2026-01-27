/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.fromList;
import static org.elasticsearch.compute.data.BlockUtils.fromListRow;

public class LocalSourceOperator extends SourceOperator {

    public record LocalSourceFactory(Supplier<LocalSourceOperator> factory) implements SourceOperatorFactory {

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return factory().get();
        }

        @Override
        public String describe() {
            return "LocalSourceOperator[" + factory + "]";
        }
    }

    public interface ObjectSupplier extends Supplier<List<Object>> {}

    public interface ListSupplier extends Supplier<List<List<Object>>> {}

    public interface BlockSupplier extends Supplier<Block[]> {}

    public interface PageSupplier extends Supplier<Page> {}

    protected final PageSupplier supplier;

    boolean finished;

    public LocalSourceOperator(BlockFactory blockFactory, ObjectSupplier objectSupplier) {
        this(() -> fromListRow(blockFactory, objectSupplier.get()));
    }

    public LocalSourceOperator(BlockFactory blockFactory, ListSupplier listSupplier) {
        this(() -> fromList(blockFactory, listSupplier.get()));
    }

    public LocalSourceOperator(BlockSupplier blockSupplier) {
        this(() -> {
            var blocks = blockSupplier.get();
            return CollectionUtils.isEmpty(blocks) ? new Page(0, blocks) : new Page(blocks);
        });
    }

    public LocalSourceOperator(PageSupplier pageSupplier) {
        this.supplier = pageSupplier;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public Page getOutput() {
        var page = supplier.get();
        finished = true;
        return page;
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "LocalSourceOperator";
    }
}
