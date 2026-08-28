/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

import java.io.IOException;
import java.util.function.Consumer;

public abstract class CpuMeteringPageIterator implements CloseableIterator<Page> {
    private StorageObject object;
    private Consumer<Long> accumulator;

    protected abstract void doClose() throws IOException;

    public void addAsyncCpuOnClose(StorageObject object, Consumer<Long> accumulator) {
        this.object = object;
        this.accumulator = accumulator;
    }

    @Override
    public void close() throws IOException {
        try {
            doClose();
        } finally {
            if (accumulator != null) {
                accumulator.accept(object.asyncCpuNanos());
            }
        }
    }
}
