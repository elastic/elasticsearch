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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestSparseInferenceServiceExtension implements InferenceServiceExtension {
    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new);
    }

    public static class TestSparseModel extends Model {
        public TestSparseModel(String inferenceEntityId, TestServiceSettings serviceSettings) {
            super(
                new ModelConfigurations(inferenceEntityId, TaskType.SPARSE_EMBEDDING, TestInferenceService.NAME, serviceSettings),
                new ModelSecrets(new AbstractTestInferenceService.TestSecretSettings("api_key"))
            );
        }
    }

    public static class TestInferenceService extends AbstractTestInferenceService {
        public static final String NAME = "test_service";

        public TestInferenceService(InferenceServiceExtension.InferenceServiceFactoryContext context) {}

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
                case ANY, SPARSE_EMBEDDING -> listener.onResponse(makeResults(input));
                default -> listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }

        @Override
        public void chunkedInfer(
            Model model,
            @Nullable String query,
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            ChunkingOptions chunkingOptions,
            TimeValue timeout,
            ActionListener<List<ChunkedInferenceServiceResults>> listener
        ) {
            switch (model.getConfigurations().getTaskType()) {
                case ANY, SPARSE_EMBEDDING -> listener.onResponse(makeChunkedResults(input));
                default -> listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }

        private SparseEmbeddingResults makeResults(List<String> input) {
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

        private List<ChunkedInferenceServiceResults> makeChunkedResults(List<String> input) {
            List<ChunkedInferenceServiceResults> results = new ArrayList<>();
            for (int i = 0; i < input.size(); i++) {
                var tokens = new ArrayList<WeightedToken>();
                for (int j = 0; j < 5; j++) {
                    tokens.add(new WeightedToken("feature_" + j, generateEmbedding(input.get(i), j)));
                }
                results.add(
                    new InferenceChunkedSparseEmbeddingResults(
                        List.of(new MlChunkedTextExpansionResults.ChunkedResult(input.get(i), tokens))
                    )
                );
            }
            return results;
        }

        protected ServiceSettings getServiceSettingsFromMap(Map<String, Object> serviceSettingsMap) {
            return TestServiceSettings.fromMap(serviceSettingsMap);
        }

        private static float generateEmbedding(String input, int position) {
            // Ensure non-negative and non-zero values for features
            return Math.abs(input.hashCode()) + 1 + position;
        }
    }

    public record TestServiceSettings(String model, String hiddenField, boolean shouldReturnHiddenField) implements ServiceSettings {

        static final String NAME = "test_service_settings";

        public static TestServiceSettings fromMap(Map<String, Object> map) {
            ValidationException validationException = new ValidationException();

            String model = (String) map.remove("model");

            if (model == null) {
                validationException.addValidationError("missing model");
            }

            String hiddenField = (String) map.remove("hidden_field");
            Boolean shouldReturnHiddenField = (Boolean) map.remove("should_return_hidden_field");

            if (shouldReturnHiddenField == null) {
                shouldReturnHiddenField = false;
            }

            if (validationException.validationErrors().isEmpty() == false) {
                throw validationException;
            }

            return new TestServiceSettings(model, hiddenField, shouldReturnHiddenField);
        }

        public TestServiceSettings(StreamInput in) throws IOException {
            this(in.readString(), in.readOptionalString(), in.readBoolean());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("model", model);
            if (hiddenField != null) {
                builder.field("hidden_field", hiddenField);
            }
            builder.field("should_return_hidden_field", shouldReturnHiddenField);
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
            out.writeOptionalString(hiddenField);
            out.writeBoolean(shouldReturnHiddenField);
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
                if (shouldReturnHiddenField && hiddenField != null) {
                    builder.field("hidden_field", hiddenField);
                }
                builder.endObject();
                return builder;
            };
        }
    }
}
