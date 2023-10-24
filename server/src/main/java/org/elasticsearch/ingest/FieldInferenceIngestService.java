/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.inference.InferenceAction;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.internal.DocumentParsingObserver;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

public class FieldInferenceIngestService extends IngestService {

    public FieldInferenceIngestService(ClusterService clusterService, ThreadPool threadPool, Environment env, ScriptService scriptService, AnalysisRegistry analysisRegistry, List<IngestPlugin> ingestPlugins, Client client, MatcherWatchdog matcherWatchdog, Supplier<DocumentParsingObserver> documentParsingObserverSupplier) {
        super(clusterService, threadPool, env, scriptService, analysisRegistry, ingestPlugins, client, matcherWatchdog, documentParsingObserverSupplier);
    }

    protected void processIndexRequest(
        IndexRequest indexRequest,
        int slot,
        RefCountingRunnable refs,
        IntConsumer onDropped,
        final BiConsumer<Integer, Exception> onFailure
    ) {

        String index = indexRequest.index();
        Map<String, Object> sourceMap = indexRequest.sourceAsMap();
        sourceMap.entrySet().stream().filter(entry -> fieldNeedsInference(index, entry.getKey())).forEach(entry -> {
            runInferenceForField(indexRequest, entry.getKey(), refs, slot, onFailure);
        });
    }

    @Override
    public boolean needsProcessing(IndexRequest indexRequest) {
        return (indexRequest.isFieldInferenceResolved() == false)
            && indexRequest.sourceAsMap().keySet().stream().anyMatch(fieldName -> fieldNeedsInference(indexRequest.index(), fieldName));
    }

    // TODO actual mapping check here
    private boolean fieldNeedsInference(String index, String fieldName) {
        return fieldName.startsWith("infer_");
    }

    private void runInferenceForField(
        IndexRequest indexRequest,
        String fieldName,
        RefCountingRunnable refs,
        int position,
        BiConsumer<Integer, Exception> onFailure
    ) {
        var ingestDocument = newIngestDocument(indexRequest, documentParsingObserverSupplier.get());
        if (ingestDocument.hasField(fieldName) == false) {
            return;
        }

        refs.acquire();

        // TODO Hardcoding model ID and task type
        InferenceAction.Request inferenceRequest = new InferenceAction.Request(
            TaskType.SPARSE_EMBEDDING,
            "my-elser-model",
            ingestDocument.getFieldValue(fieldName, String.class),
            Map.of()
        );

        client.execute(InferenceAction.INSTANCE, inferenceRequest, new ActionListener<InferenceAction.Response>() {
            @Override
            public void onResponse(InferenceAction.Response response) {
                ingestDocument.setFieldValue(fieldName + "_inference", response.getResult().asMap(fieldName).get(fieldName));
                updateIndexRequestSource(indexRequest, ingestDocument);
                indexRequest.isFieldInferenceResolved(true);
                refs.close();
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(position, e);
                refs.close();
            }
        });
    }
}
