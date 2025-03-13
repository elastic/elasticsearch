/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.persistence;

import org.elasticsearch.index.Index;

import java.util.List;

final class IndexState<T extends ProfilingIndexAbstraction> {
    private final T index;
    private final Index writeIndex;
    private final IndexStatus status;
    private final List<Migration> pendingMigrations;

    IndexState(T index, Index writeIndex, IndexStatus status) {
        this(index, writeIndex, status, null);
    }

    IndexState(T index, Index writeIndex, IndexStatus status, List<Migration> pendingMigrations) {
        this.index = index;
        this.writeIndex = writeIndex;
        this.status = status;
        this.pendingMigrations = pendingMigrations;
    }

    public T getIndex() {
        return index;
    }

    public Index getWriteIndex() {
        return writeIndex;
    }

    public IndexStatus getStatus() {
        return status;
    }

    public List<Migration> getPendingMigrations() {
        return pendingMigrations;
    }
}
