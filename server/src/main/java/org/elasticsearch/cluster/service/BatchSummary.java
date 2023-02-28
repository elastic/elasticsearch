/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.common.util.LazyInitializable;

import java.util.function.Supplier;

public class BatchSummary {

    private final LazyInitializable<String, RuntimeException> lazyDescription;

    public BatchSummary(Supplier<String> stringSupplier) {
        lazyDescription = new LazyInitializable<>(stringSupplier::get);
    }

    @Override
    public String toString() {
        return lazyDescription.getOrCompute();
    }
}
