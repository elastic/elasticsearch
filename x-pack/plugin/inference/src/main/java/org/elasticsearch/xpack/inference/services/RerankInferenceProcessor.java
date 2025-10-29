/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.xpack.core.inference.chunking.RerankRequestChunker;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.services.settings.LongDocumentStrategy;
import org.elasticsearch.xpack.inference.services.settings.RerankServiceSettings;

public class RerankInferenceProcessor {
    public static void doInfer(
        SenderService service,
        Model model,
        ExecutableAction action,
        InferenceInputs inputs,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var serviceSettings = model.getServiceSettings();
        if (serviceSettings instanceof RerankServiceSettings == false) {
            throw new IllegalArgumentException("RerankInferenceProcessor can only process models with RerankServiceSettings");
        }

        var rerankServiceSettings = (RerankServiceSettings) serviceSettings;
        if (LongDocumentStrategy.CHUNK.equals(rerankServiceSettings.getLongDocumentStrategy())) {
            if (inputs instanceof QueryAndDocsInputs == false) {
                throw new IllegalArgumentException("RerankInferenceProcessor can only process QueryAndDocsInputs when chunking is enabled");
            }
            var queryAndDocsInputs = (QueryAndDocsInputs) inputs;

            if (service instanceof RerankingInferenceService == false) {
                throw new IllegalArgumentException(
                    "RerankInferenceProcessor can only process RerankingInferenceService when chunking is enabled"
                );
            }

            var rerankRequestChunker = new RerankRequestChunker(
                queryAndDocsInputs.getQuery(),
                queryAndDocsInputs.getChunks(),
                ((RerankingInferenceService) service).rerankerWindowSize(serviceSettings.modelId()),
                rerankServiceSettings.getMaxChunksPerDoc()
            );

            inputs = new QueryAndDocsInputs(
                queryAndDocsInputs.getQuery(),
                rerankRequestChunker.getChunkedInputs(),
                queryAndDocsInputs.getReturnDocuments(), // TODO: Check if we want this from inputs or from task settings
                queryAndDocsInputs.getTopN(), // TODO: Check if we want this from inputs or from task settings
                queryAndDocsInputs.stream()
            );

            listener = rerankRequestChunker.parseChunkedRerankResultsListener(
                listener,
                queryAndDocsInputs.getReturnDocuments() == null || queryAndDocsInputs.getReturnDocuments(),
                queryAndDocsInputs.getTopN()
            );
        }
        action.execute(inputs, timeout, listener);
    }
}
