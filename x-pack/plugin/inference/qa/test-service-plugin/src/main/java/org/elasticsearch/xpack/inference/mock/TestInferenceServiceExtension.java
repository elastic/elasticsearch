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
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.ChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestInferenceServiceExtension implements InferenceServiceExtension {
    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new);
    }

    public static class TestInferenceService implements InferenceService {
        private static final String NAME = "test_service";

        public TestInferenceService(InferenceServiceExtension.InferenceServiceFactoryContext context) {}

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current(); // fine for these tests but will not work for cluster upgrade tests
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Object> getTaskSettingsMap(Map<String, Object> settings) {
            Map<String, Object> taskSettingsMap;
            // task settings are optional
            if (settings.containsKey(ModelConfigurations.TASK_SETTINGS)) {
                taskSettingsMap = (Map<String, Object>) settings.remove(ModelConfigurations.TASK_SETTINGS);
            } else {
                taskSettingsMap = Map.of();
            }

            return taskSettingsMap;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void parseRequestConfig(
            String modelId,
            TaskType taskType,
            Map<String, Object> config,
            Set<String> platfromArchitectures,
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
        @SuppressWarnings("unchecked")
        public TestServiceModel parsePersistedConfigWithSecrets(
            String modelId,
            TaskType taskType,
            Map<String, Object> config,
            Map<String, Object> secrets
        ) {
            var serviceSettingsMap = (Map<String, Object>) config.remove(ModelConfigurations.SERVICE_SETTINGS);
            var secretSettingsMap = (Map<String, Object>) secrets.remove(ModelSecrets.SECRET_SETTINGS);

            var serviceSettings = TestServiceSettings.fromMap(serviceSettingsMap);
            var secretSettings = TestSecretSettings.fromMap(secretSettingsMap);

            var taskSettingsMap = getTaskSettingsMap(config);
            var taskSettings = TestTaskSettings.fromMap(taskSettingsMap);

            return new TestServiceModel(modelId, taskType, name(), serviceSettings, taskSettings, secretSettings);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
            var serviceSettingsMap = (Map<String, Object>) config.remove(ModelConfigurations.SERVICE_SETTINGS);

            var serviceSettings = TestServiceSettings.fromMap(serviceSettingsMap);

            var taskSettingsMap = getTaskSettingsMap(config);
            var taskSettings = TestTaskSettings.fromMap(taskSettingsMap);

            return new TestServiceModel(modelId, taskType, name(), serviceSettings, taskSettings, null);
        }

        @Override
        public void infer(
            Model model,
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
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
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            ChunkingOptions chunkingOptions,
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
                var tokens = new ArrayList<SparseEmbeddingResults.WeightedToken>();
                for (int j = 0; j < 5; j++) {
                    tokens.add(new SparseEmbeddingResults.WeightedToken(Integer.toString(j), (float) j));
                }
                embeddings.add(new SparseEmbeddingResults.Embedding(tokens, false));
            }
            return new SparseEmbeddingResults(embeddings);
        }

        private List<ChunkedInferenceServiceResults> makeChunkedResults(List<String> input) {
            var chunks = new ArrayList<ChunkedTextExpansionResults.ChunkedResult>();
            for (int i = 0; i < input.size(); i++) {
                var tokens = new ArrayList<TextExpansionResults.WeightedToken>();
                for (int j = 0; j < 5; j++) {
                    tokens.add(new TextExpansionResults.WeightedToken(Integer.toString(j), (float) j));
                }
                chunks.add(new ChunkedTextExpansionResults.ChunkedResult(input.get(i), tokens));
            }
            return List.of(new ChunkedSparseEmbeddingResults(chunks));
        }

        @Override
        public void start(Model model, ActionListener<Boolean> listener) {
            listener.onResponse(true);
        }

        @Override
        public void close() throws IOException {}
    }

    public static class TestServiceModel extends Model {

        public TestServiceModel(
            String modelId,
            TaskType taskType,
            String service,
            TestServiceSettings serviceSettings,
            TestTaskSettings taskSettings,
            TestSecretSettings secretSettings
        ) {
            super(new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secretSettings));
        }

        @Override
        public TestServiceSettings getServiceSettings() {
            return (TestServiceSettings) super.getServiceSettings();
        }

        @Override
        public TestTaskSettings getTaskSettings() {
            return (TestTaskSettings) super.getTaskSettings();
        }

        @Override
        public TestSecretSettings getSecretSettings() {
            return (TestSecretSettings) super.getSecretSettings();
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

    public record TestTaskSettings(Integer temperature) implements TaskSettings {

        static final String NAME = "test_task_settings";

        public static TestTaskSettings fromMap(Map<String, Object> map) {
            Integer temperature = (Integer) map.remove("temperature");
            return new TestTaskSettings(temperature);
        }

        public TestTaskSettings(StreamInput in) throws IOException {
            this(in.readOptionalVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalVInt(temperature);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (temperature != null) {
                builder.field("temperature", temperature);
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
    }

    public record TestSecretSettings(String apiKey) implements SecretSettings {

        static final String NAME = "test_secret_settings";

        public static TestSecretSettings fromMap(Map<String, Object> map) {
            ValidationException validationException = new ValidationException();

            String apiKey = (String) map.remove("api_key");

            if (apiKey == null) {
                validationException.addValidationError("missing api_key");
            }

            if (validationException.validationErrors().isEmpty() == false) {
                throw validationException;
            }

            return new TestSecretSettings(apiKey);
        }

        public TestSecretSettings(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(apiKey);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("api_key", apiKey);
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
    }
}
