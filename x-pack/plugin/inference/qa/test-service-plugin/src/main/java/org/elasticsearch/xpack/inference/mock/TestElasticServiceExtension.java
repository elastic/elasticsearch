/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mock;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Using hardcoded string due to module visibility

/**
 * Mock service extension that simulates Elastic Inference Service (EIS) behavior
 * for testing semantic_text field mapping with default endpoints.
 */
public class TestElasticServiceExtension implements InferenceServiceExtension {

    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestElasticInferenceService::new);
    }

    public static class TestElasticInferenceService extends AbstractTestInferenceService {
        public static final String NAME = "test_elastic_service";

        private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(
            TaskType.SPARSE_EMBEDDING,
            TaskType.TEXT_EMBEDDING,
            TaskType.RERANK
        );

        public TestElasticInferenceService(InferenceServiceExtension.InferenceServiceFactoryContext context) {}

        @Override
        public String name() {
            return NAME;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void parseRequestConfig(
            String modelId,
            TaskType taskType,
            Map<String, Object> config,
            ActionListener<Model> parsedModelListener
        ) {
            var serviceSettingsMap = (Map<String, Object>) config.remove(ModelConfigurations.SERVICE_SETTINGS);
            var serviceSettings = TestServiceSettings.fromMap(serviceSettingsMap);
            var secretSettings = TestSecretSettings.fromMap(serviceSettingsMap);

            var taskSettingsMap = getTaskSettingsMap(config);
            var taskSettings = TestTaskSettings.fromMap(taskSettingsMap);

            parsedModelListener.onResponse(new TestServiceModel(modelId, taskType, name(), serviceSettings, taskSettings, secretSettings));
        }

        @Override
        public InferenceServiceConfiguration getConfiguration() {
            return Configuration.get();
        }

        @Override
        public EnumSet<TaskType> supportedTaskTypes() {
            return supportedTaskTypes;
        }

        @Override
        public List<DefaultConfigId> defaultConfigIds() {
            return List.of(
                new DefaultConfigId(".elser-2-elastic", MinimalServiceSettings.sparseEmbedding(NAME), this),
                new DefaultConfigId(
                    ".multilingual-embed-v1-elastic",
                    MinimalServiceSettings.textEmbedding(NAME, 1024, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                    this
                ),
                new DefaultConfigId(".rerank-v1-elastic", MinimalServiceSettings.rerank(NAME), this)
            );
        }

        @Override
        public void infer(
            Model model,
            @Nullable String query,
            @Nullable Boolean returnDocuments,
            @Nullable Integer topN,
            List<String> input,
            boolean stream,
            Map<String, Object> taskSettings,
            InputType inputType,
            TimeValue timeout,
            ActionListener<InferenceServiceResults> listener
        ) {
            switch (model.getConfigurations().getTaskType()) {
                case SPARSE_EMBEDDING -> listener.onResponse(makeSparseResults(input));
                case TEXT_EMBEDDING -> listener.onResponse(makeDenseResults(input));
                case RERANK -> listener.onResponse(makeRerankResults(input));
                default -> listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }

        @Override
        public void unifiedCompletionInfer(
            Model model,
            UnifiedCompletionRequest request,
            TimeValue timeout,
            ActionListener<InferenceServiceResults> listener
        ) {
            throw new UnsupportedOperationException("unifiedCompletionInfer not supported");
        }

        @Override
        public void chunkedInfer(
            Model model,
            @Nullable String query,
            List<ChunkInferenceInput> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            TimeValue timeout,
            ActionListener<List<ChunkedInference>> listener
        ) {
            switch (model.getConfigurations().getTaskType()) {
                case SPARSE_EMBEDDING -> listener.onResponse(makeChunkedSparseResults(input));
                case TEXT_EMBEDDING -> listener.onResponse(makeChunkedDenseResults(input));
                case RERANK -> listener.onFailure(
                    new ElasticsearchStatusException(
                        "Chunked inference is not supported for rerank task type",
                        RestStatus.BAD_REQUEST
                    )
                );
                default -> listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }

        private SparseEmbeddingResults makeSparseResults(List<String> input) {
            var embeddings = new ArrayList<SparseEmbeddingResults.Embedding>();
            for (int i = 0; i < input.size(); i++) {
                var tokens = new ArrayList<WeightedToken>();
                for (int j = 0; j < 5; j++) {
                    tokens.add(new WeightedToken("feature_" + j, generateEmbedding(input.get(i), j)));
                }
                embeddings.add(new SparseEmbeddingResults.Embedding(tokens, false));
            }
            return new SparseEmbeddingResults(embeddings);
        }

        private TextEmbeddingFloatResults makeDenseResults(List<String> input) {
            var embeddings = new ArrayList<TextEmbeddingFloatResults.Embedding>();
            for (String text : input) {
                float[] embedding = new float[1024];
                for (int i = 0; i < embedding.length; i++) {
                    embedding[i] = generateEmbedding(text, i);
                }
                embeddings.add(new TextEmbeddingFloatResults.Embedding(embedding));
            }
            return new TextEmbeddingFloatResults(embeddings);
        }

        private List<ChunkedInference> makeChunkedSparseResults(List<ChunkInferenceInput> inputs) {
            List<ChunkedInference> results = new ArrayList<>();
            for (ChunkInferenceInput chunkInferenceInput : inputs) {
                List<ChunkedInput> chunkedInput = chunkInputs(chunkInferenceInput);
                List<SparseEmbeddingResults.Chunk> chunks = chunkedInput.stream().map(c -> {
                    var tokens = new ArrayList<WeightedToken>();
                    for (int i = 0; i < 5; i++) {
                        tokens.add(new WeightedToken("feature_" + i, generateEmbedding(c.input(), i)));
                    }
                    var embeddings = new SparseEmbeddingResults.Embedding(tokens, false);
                    return new SparseEmbeddingResults.Chunk(embeddings, new ChunkedInference.TextOffset(c.startOffset(), c.endOffset()));
                }).toList();
                ChunkedInferenceEmbedding chunkedInferenceEmbedding = new ChunkedInferenceEmbedding(chunks);
                results.add(chunkedInferenceEmbedding);
            }
            return results;
        }

        private List<ChunkedInference> makeChunkedDenseResults(List<ChunkInferenceInput> inputs) {
            List<ChunkedInference> results = new ArrayList<>();
            for (ChunkInferenceInput chunkInferenceInput : inputs) {
                List<ChunkedInput> chunkedInput = chunkInputs(chunkInferenceInput);
                List<TextEmbeddingFloatResults.Chunk> chunks = chunkedInput.stream().map(c -> {
                    float[] embedding = new float[1024];
                    for (int i = 0; i < embedding.length; i++) {
                        embedding[i] = generateEmbedding(c.input(), i);
                    }
                    var embeddings = new TextEmbeddingFloatResults.Embedding(embedding);
                    return new TextEmbeddingFloatResults.Chunk(embeddings, new ChunkedInference.TextOffset(c.startOffset(), c.endOffset()));
                }).toList();
                ChunkedInferenceEmbedding chunkedInferenceEmbedding = new ChunkedInferenceEmbedding(chunks);
                results.add(chunkedInferenceEmbedding);
            }
            return results;
        }

        protected ServiceSettings getServiceSettingsFromMap(Map<String, Object> serviceSettingsMap) {
            return TestServiceSettings.fromMap(serviceSettingsMap);
        }

        private RankedDocsResults makeRerankResults(List<String> input) {
            var rankedDocs = new ArrayList<RankedDocsResults.RankedDoc>();
            for (int i = 0; i < input.size(); i++) {
                float relevanceScore = 1.0f - (i * 0.1f); // Decreasing relevance scores
                rankedDocs.add(new RankedDocsResults.RankedDoc(i, Math.max(0.1f, relevanceScore), input.get(i)));
            }
            return new RankedDocsResults(rankedDocs);
        }

        private static float generateEmbedding(String input, int position) {
            return Math.abs(input.hashCode()) + 1 + position;
        }

        public static class Configuration {
            public static InferenceServiceConfiguration get() {
                return configuration.getOrCompute();
            }

            private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
                () -> {
                    var configurationMap = new HashMap<String, SettingsConfiguration>();

                    configurationMap.put(
                        "model",
                        new SettingsConfiguration.Builder(supportedTaskTypes).setDescription("")
                            .setLabel("Model")
                            .setRequired(true)
                            .setSensitive(false)
                            .setType(SettingsConfigurationFieldType.STRING)
                            .build()
                    );

                    return new InferenceServiceConfiguration.Builder().setService(NAME)
                        .setName(NAME)
                        .setTaskTypes(supportedTaskTypes)
                        .setConfigurations(configurationMap)
                        .build();
                }
            );
        }
    }

    public record TestServiceSettings(String model) implements ServiceSettings {

        static final String NAME = "test_elastic_service_settings";

        public static TestServiceSettings fromMap(Map<String, Object> map) {
            ValidationException validationException = new ValidationException();

            String model = (String) map.remove("model");
            if (model == null) {
                validationException.addValidationError("missing model");
            }

            if (validationException.validationErrors().isEmpty() == false) {
                throw validationException;
            }

            return new TestServiceSettings(model);
        }

        public TestServiceSettings(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("model", model);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(model);
        }

        @Override
        public String modelId() {
            return model;
        }

        @Override
        public ToXContentObject getFilteredXContentObject() {
            return (builder, params) -> {
                builder.startObject();
                builder.field("model", model);
                builder.endObject();
                return builder;
            };
        }
    }
}
