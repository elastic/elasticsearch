/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.Metadata;

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

public interface BulkRequestPreprocessor {
    void processBulkRequest(
        ExecutorService executorService,
        int numberOfActionRequests,
        Iterable<DocWriteRequest<?>> actionRequests,
        IntConsumer onDropped,
        BiConsumer<Integer, Exception> onFailure,
        BiConsumer<Thread, Exception> onCompletion,
        String executorName
    );

    boolean needsProcessing(DocWriteRequest<?> docWriteRequest, IndexRequest indexRequest, Metadata metadata);

    boolean hasBeenProcessed(IndexRequest indexRequest);

    boolean shouldExecuteOnIngestNode();
}
