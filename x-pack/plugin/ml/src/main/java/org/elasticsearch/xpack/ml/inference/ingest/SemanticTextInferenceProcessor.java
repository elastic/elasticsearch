/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ingest;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.WrappingProcessor;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.ml.mapper.SemanticTextInferenceResultFieldMapper;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class SemanticTextInferenceProcessor extends AbstractProcessor implements WrappingProcessor {

    public static final String TYPE = "semanticTextInference";
    public static final String TAG = "semantic_text";

    private final Map<String, Set<String>> fieldsForModels;

    private final Processor wrappedProcessor;

    private final Client client;
    private final InferenceAuditor inferenceAuditor;

    public SemanticTextInferenceProcessor(
        Client client,
        InferenceAuditor inferenceAuditor,
        String description,
        Map<String, Set<String>> fieldsForModels
    ) {
        super(TAG, description);
        this.client = client;
        this.inferenceAuditor = inferenceAuditor;

        this.fieldsForModels = fieldsForModels;
        this.wrappedProcessor = createWrappedProcessor();
    }

    private Processor createWrappedProcessor() {
        InferenceProcessor[] inferenceProcessors = fieldsForModels.entrySet()
            .stream()
            .map(e -> createInferenceProcessor(e.getKey(), e.getValue()))
            .toArray(InferenceProcessor[]::new);
        return new CompoundProcessor(inferenceProcessors);
    }

    private InferenceProcessor createInferenceProcessor(String modelId, Set<String> fields) {
        List<InferenceProcessor.Factory.InputConfig> inputConfigs = fields.stream()
            .map(f -> new InferenceProcessor.Factory.InputConfig(f, SemanticTextInferenceResultFieldMapper.NAME, f, Map.of()))
            .toList();

        return InferenceProcessor.fromInputFieldConfiguration(
            client,
            inferenceAuditor,
            tag,
            "inference processor for semantic text",
            modelId,
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            inputConfigs,
            false
        );
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        getInnerProcessor().execute(ingestDocument, handler);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        return getInnerProcessor().execute(ingestDocument);
    }

    @Override
    public Processor getInnerProcessor() {
        return wrappedProcessor;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
