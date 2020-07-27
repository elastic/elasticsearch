/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexingPressure;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * A helper class that gathers index requests in bulk requests
 * that do exceed a 1000 operations or half the available memory
 * limit for indexing.
 */
public class MlBulkIndexer implements AutoCloseable {

    private static final int BATCH_SIZE = 1000;

    private final long bytesLimit;
    private final Consumer<BulkRequest> executor;
    private BulkRequest currentBulkRequest = new BulkRequest();
    private long currentRamBytes;

    public MlBulkIndexer(Settings settings, Consumer<BulkRequest> executor) {
        this((long) Math.ceil(0.5 * IndexingPressure.MAX_INDEXING_BYTES.get(settings).getBytes()), executor);
    }

    MlBulkIndexer(long bytesLimit, Consumer<BulkRequest> executor) {
        this.bytesLimit = bytesLimit;
        this.executor = Objects.requireNonNull(executor);
    }

    public void addAndExecuteIfNeeded(IndexRequest indexRequest) {
        if (currentRamBytes + indexRequest.ramBytesUsed() > bytesLimit || currentBulkRequest.numberOfActions() == BATCH_SIZE) {
            execute();
        }
        currentBulkRequest.add(indexRequest);
        currentRamBytes += indexRequest.ramBytesUsed();
    }

    private void execute() {
        if (currentBulkRequest.numberOfActions() > 0) {
            executor.accept(currentBulkRequest);
            currentBulkRequest = new BulkRequest();
            currentRamBytes = 0;
        }
    }

    @Override
    public void close() {
        execute();
    }
}
