/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.compute.data.Block;

import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

public interface LocalSupplier extends Supplier<List<Block>> {

    LocalSupplier EMPTY = new LocalSupplier() {
        @Override
        public List<Block> get() {
            return emptyList();
        }
    };
}
