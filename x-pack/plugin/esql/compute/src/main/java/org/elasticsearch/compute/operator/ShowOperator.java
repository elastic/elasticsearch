/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.data.Page;

public class ShowOperator extends LocalSourceOperator {

    public record Factory(BlockSupplier blockSupplier, String source) implements SourceOperatorFactory {
        @Override
        public String describe() {
            return ShowOperator.class.getSimpleName() + "[" + source + "]";
        }

        @Override
        public SourceOperator get(DriverContext unused) {
            return new ShowOperator(blockSupplier);
        }
    }

    public ShowOperator(BlockSupplier blockSupplier) {
        super(() -> {
            var blocks = blockSupplier.get();
            if (blocks == null) {
                return null;
            }
            return CollectionUtils.isEmpty(blocks) ? new Page(0, blocks) : new Page(blocks);
        });
    }

    @Override
    public Page getOutput() {
        Page page = supplier.get();
        finished = page != null;
        return page;
    }
}
