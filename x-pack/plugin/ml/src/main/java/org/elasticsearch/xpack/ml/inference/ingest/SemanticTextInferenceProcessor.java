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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class SemanticTextInferenceProcessor extends AbstractProcessor implements WrappingProcessor {

    public static final String TYPE = "semanticTextInference";
    public static final String TAG = "semantic_text";
    public static final String TEXT_SUFFIX = ".text";
    public static final String INFERENCE_SUFFIX = ".inference";

    private final Map<String, Set<String>> modelForFields;

    private final Processor wrappedProcessor;

    private final Client client;
    private final InferenceAuditor inferenceAuditor;

    public SemanticTextInferenceProcessor(
        Client client,
        InferenceAuditor inferenceAuditor,
        String description,
        Map<String, Set<String>> modelForFields
    ) {
        super(TAG, description);
        this.client = client;
        this.inferenceAuditor = inferenceAuditor;

        this.modelForFields = modelForFields;
        this.wrappedProcessor = createWrappedProcessor();
    }

    private Processor createWrappedProcessor() {
        InferenceProcessor[] inferenceProcessors = modelForFields.entrySet()
            .stream()
            .map(e -> createInferenceProcessor(e.getKey(), e.getValue()))
            .toArray(InferenceProcessor[]::new);
        return new CompoundProcessor(inferenceProcessors);
    }

    private InferenceProcessor createInferenceProcessor(String modelId, Set<String> fields) {
        List<InferenceProcessor.Factory.InputConfig> inputConfigs = fields.stream()
            .map(field -> new InferenceProcessor.Factory.InputConfig(
                SemanticTextInferenceResultFieldMapper.NAME + "." + field,
                SemanticTextInferenceResultFieldMapper.NAME,
                field,
                Map.of()))
            .toList();

        return InferenceProcessor.fromInputFieldConfiguration(
            client,
            inferenceAuditor,
            tag,
            "inference processor for semantic text",
            modelId,
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            inputConfigs,
            false,
            true
        );
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        modelForFields.forEach((modelId, fields) -> chunkText(ingestDocument, modelId, fields));
        getInnerProcessor().execute(ingestDocument, handler);
    }

    private static void chunkText(IngestDocument ingestDocument, String modelId, Set<String> fields) {
        for (String field : fields) {
            String value = ingestDocument.getFieldValue(field, String.class);
            if (value != null) {
                String[] chunks = value.split("\\.");
                ingestDocument.setFieldValue(SemanticTextInferenceResultFieldMapper.NAME + "." + field, new ArrayList<>());
                for (String chunk : chunks) {
                    ingestDocument.appendFieldValue(SemanticTextInferenceResultFieldMapper.NAME + "." + field, Map.of("text", chunk));
                }
            }
        }
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
