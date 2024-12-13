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
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.EmptySettingsConfiguration;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskSettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbeddingFloat;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDenseInferenceServiceExtension implements InferenceServiceExtension {
    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new);
    }

    public static class TestDenseModel extends Model {
        public TestDenseModel(String inferenceEntityId, TestDenseInferenceServiceExtension.TestServiceSettings serviceSettings) {
            super(
                new ModelConfigurations(
                    inferenceEntityId,
                    TaskType.TEXT_EMBEDDING,
                    TestDenseInferenceServiceExtension.TestInferenceService.NAME,
                    serviceSettings
                ),
                new ModelSecrets(new AbstractTestInferenceService.TestSecretSettings("api_key"))
            );
        }
    }

    public static class TestInferenceService extends AbstractTestInferenceService {
        public static final String NAME = "text_embedding_test_service";

        private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(TaskType.TEXT_EMBEDDING);

        public TestInferenceService(InferenceServiceFactoryContext context) {}

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
        public void infer(
            Model model,
            @Nullable String query,
            List<String> input,
            boolean stream,
            Map<String, Object> taskSettings,
            InputType inputType,
            TimeValue timeout,
            ActionListener<InferenceServiceResults> listener
        ) {
            switch (model.getConfigurations().getTaskType()) {
                case ANY, TEXT_EMBEDDING -> {
                    ServiceSettings modelServiceSettings = model.getServiceSettings();
                    listener.onResponse(makeResults(input, modelServiceSettings.dimensions()));
                }
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
            listener.onFailure(new UnsupportedOperationException("unifiedCompletionInfer not supported"));
        }

        @Override
        public void chunkedInfer(
            Model model,
            @Nullable String query,
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            TimeValue timeout,
            ActionListener<List<ChunkedInference>> listener
        ) {
            switch (model.getConfigurations().getTaskType()) {
                case ANY, TEXT_EMBEDDING -> {
                    ServiceSettings modelServiceSettings = model.getServiceSettings();
                    listener.onResponse(makeChunkedResults(input, modelServiceSettings.dimensions()));
                }
                default -> listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }

        private InferenceTextEmbeddingFloatResults makeResults(List<String> input, int dimensions) {
            List<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding> embeddings = new ArrayList<>();
            for (String inputString : input) {
                List<Float> floatEmbeddings = generateEmbedding(inputString, dimensions);
                embeddings.add(InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding.of(floatEmbeddings));
            }
            return new InferenceTextEmbeddingFloatResults(embeddings);
        }

        private List<ChunkedInference> makeChunkedResults(List<String> input, int dimensions) {
            InferenceTextEmbeddingFloatResults nonChunkedResults = makeResults(input, dimensions);

            var results = new ArrayList<ChunkedInference>();
            for (int i = 0; i < input.size(); i++) {
                results.add(
                    new ChunkedInferenceEmbeddingFloat(
                        List.of(
                            new ChunkedInferenceEmbeddingFloat.FloatEmbeddingChunk(
                                nonChunkedResults.embeddings().get(i).values(),
                                input.get(i),
                                new ChunkedInference.TextOffset(0, input.get(i).length())
                            )
                        )
                    )
                );
            }
            return results;
        }

        protected ServiceSettings getServiceSettingsFromMap(Map<String, Object> serviceSettingsMap) {
            return TestServiceSettings.fromMap(serviceSettingsMap);
        }

        /**
         * Generate a test embedding for the provided input.
         * <p>
         * The goal of this method is to generate an embedding with the following properties:
         * </p>
         * <ul>
         *     <li>Unique to the input</li>
         *     <li>Reproducible (i.e given the same input, the same embedding should be generated)</li>
         *     <li>Valid as both a float and byte embedding</li>
         * </ul>
         * <p>
         * The embedding is generated by:
         * </p>
         * <ul>
         *     <li>getting the hash code of the input</li>
         *     <li>converting the hash code value to a string</li>
         *     <li>converting the string to a UTF-8 encoded byte array</li>
         *     <li>repeatedly appending the byte array to the embedding until the desired number of dimensions are populated</li>
         * </ul>
         * <p>
         * Since the hash code value, when interpreted as a string, is guaranteed to only contain digits and the "-" character, the UTF-8
         * encoded byte array is guaranteed to only contain values in the standard ASCII table.
         * </p>
         *
         * @param input The input string
         * @param dimensions The embedding dimension count
         * @return An embedding
         */
        private static List<Float> generateEmbedding(String input, int dimensions) {
            List<Float> embedding = new ArrayList<>(dimensions);

            byte[] byteArray = Integer.toString(input.hashCode()).getBytes(StandardCharsets.UTF_8);
            List<Float> embeddingValues = new ArrayList<>(byteArray.length);
            for (byte value : byteArray) {
                embeddingValues.add((float) value);
            }

            int remainingDimensions = dimensions;
            while (remainingDimensions >= embeddingValues.size()) {
                embedding.addAll(embeddingValues);
                remainingDimensions -= embeddingValues.size();
            }
            if (remainingDimensions > 0) {
                embedding.addAll(embeddingValues.subList(0, remainingDimensions));
            }

            return embedding;
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
                        new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TEXTBOX)
                            .setLabel("Model")
                            .setOrder(1)
                            .setRequired(true)
                            .setSensitive(true)
                            .setTooltip("")
                            .setType(SettingsConfigurationFieldType.STRING)
                            .build()
                    );

                    return new InferenceServiceConfiguration.Builder().setProvider(NAME).setTaskTypes(supportedTaskTypes.stream().map(t -> {
                        Map<String, SettingsConfiguration> taskSettingsConfig;
                        switch (t) {
                            default -> taskSettingsConfig = EmptySettingsConfiguration.get();
                        }
                        return new TaskSettingsConfiguration.Builder().setTaskType(t).setConfiguration(taskSettingsConfig).build();
                    }).toList()).setConfiguration(configurationMap).build();
                }
            );
        }
    }

    public record TestServiceSettings(
        String model,
        Integer dimensions,
        SimilarityMeasure similarity,
        DenseVectorFieldMapper.ElementType elementType
    ) implements ServiceSettings {

        static final String NAME = "test_text_embedding_service_settings";

        public TestServiceSettings {
            if (elementType == DenseVectorFieldMapper.ElementType.BIT) {
                throw new IllegalArgumentException("Test dense inference service does not yet support element type BIT");
            }
        }

        public static TestServiceSettings fromMap(Map<String, Object> map) {
            ValidationException validationException = new ValidationException();

            String model = (String) map.remove("model");
            if (model == null) {
                validationException.addValidationError("missing model");
            }

            Integer dimensions = (Integer) map.remove("dimensions");
            if (dimensions == null) {
                validationException.addValidationError("missing dimensions");
            }

            SimilarityMeasure similarity = null;
            String similarityStr = (String) map.remove("similarity");
            if (similarityStr != null) {
                similarity = SimilarityMeasure.fromString(similarityStr);
            }

            DenseVectorFieldMapper.ElementType elementType = null;
            String elementTypeStr = (String) map.remove("element_type");
            if (elementTypeStr != null) {
                elementType = DenseVectorFieldMapper.ElementType.fromString(elementTypeStr);
            }

            if (validationException.validationErrors().isEmpty() == false) {
                throw validationException;
            }

            return new TestServiceSettings(model, dimensions, similarity, elementType);
        }

        public TestServiceSettings(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readOptionalInt(),
                in.readOptionalEnum(SimilarityMeasure.class),
                in.readOptionalEnum(DenseVectorFieldMapper.ElementType.class)
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("model", model);
            builder.field("dimensions", dimensions);
            if (similarity != null) {
                builder.field("similarity", similarity);
            }
            if (elementType != null) {
                builder.field("element_type", elementType);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current(); // fine for these tests but will not work for cluster upgrade tests
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(model);
            out.writeInt(dimensions);
            out.writeOptionalEnum(similarity);
            out.writeOptionalEnum(elementType);
        }

        @Override
        public ToXContentObject getFilteredXContentObject() {
            return this;
        }

        @Override
        public SimilarityMeasure similarity() {
            return similarity != null ? similarity : SimilarityMeasure.COSINE;
        }

        @Override
        public DenseVectorFieldMapper.ElementType elementType() {
            return elementType != null ? elementType : DenseVectorFieldMapper.ElementType.FLOAT;
        }

        @Override
        public String modelId() {
            return model;
        }
    }

}
