/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.TrackingResultProcessor.decorate;

class SimulateExecutionService {

    private static final String THREAD_POOL_NAME = ThreadPool.Names.MANAGEMENT;

    private final ThreadPool threadPool;

    SimulateExecutionService(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    static void executeDocument(
        Pipeline pipeline,
        IngestDocument ingestDocument,
        boolean verbose,
        BiConsumer<SimulateDocumentResult, Exception> handler
    ) {
        if (verbose) {
            List<SimulateProcessorResult> processorResultList = new CopyOnWriteArrayList<>();
            CompoundProcessor verbosePipelineProcessor = decorate(pipeline.getCompoundProcessor(), null, processorResultList);
            Pipeline verbosePipeline = new Pipeline(
                pipeline.getId(),
                pipeline.getDescription(),
                pipeline.getVersion(),
                pipeline.getMetadata(),
                verbosePipelineProcessor
            );
            ingestDocument.executePipeline(
                verbosePipeline,
                (result, e) -> { handler.accept(new SimulateDocumentVerboseResult(processorResultList), e); }
            );
        } else {
            ingestDocument.executePipeline(pipeline, (result, e) -> {
                if (e == null) {
                    handler.accept(new SimulateDocumentBaseResult(result), null);
                } else {
                    handler.accept(new SimulateDocumentBaseResult(e), null);
                }
            });
        }
    }

    public void execute(SimulatePipelineRequest.Parsed request, ActionListener<SimulatePipelineResponse> listener) {
        threadPool.executor(THREAD_POOL_NAME).execute(ActionRunnable.wrap(listener, l -> {
            final AtomicInteger counter = new AtomicInteger();
            final List<SimulateDocumentResult> responses = new CopyOnWriteArrayList<>(
                new SimulateDocumentBaseResult[request.documents().size()]
            );

            if (request.documents().isEmpty()) {
                l.onResponse(new SimulatePipelineResponse(request.pipeline().getId(), request.verbose(), responses));
                return;
            }

            int iter = 0;
            for (IngestDocument ingestDocument : request.documents()) {
                final int index = iter;
                executeDocument(request.pipeline(), ingestDocument, request.verbose(), (response, e) -> {
                    if (response != null) {
                        responses.set(index, response);
                    }
                    if (counter.incrementAndGet() == request.documents().size()) {
                        l.onResponse(new SimulatePipelineResponse(request.pipeline().getId(), request.verbose(), responses));
                    }
                });
                iter++;
            }
        }));
    }
}
