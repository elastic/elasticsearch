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
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TestCompletionServiceExtension implements InferenceServiceExtension {
    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new);
    }

    public static class TestInferenceService extends AbstractTestInferenceService {
        private static final String NAME = "completion_test_service";
        private static final EnumSet<TaskType> SUPPORTED_TASK_TYPES = EnumSet.of(TaskType.COMPLETION);

        public TestInferenceService(InferenceServiceFactoryContext context) {}

        @Override
        public String name() {
            return NAME;
        }

        @Override
        protected ServiceSettings getServiceSettingsFromMap(Map<String, Object> serviceSettingsMap) {
            return TestServiceSettings.fromMap(serviceSettingsMap);
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
            var serviceSettings = TestSparseInferenceServiceExtension.TestServiceSettings.fromMap(serviceSettingsMap);
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
            return SUPPORTED_TASK_TYPES;
        }

        @Override
        public void infer(
            Model model,
            String query,
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
                case COMPLETION -> listener.onResponse(makeChatCompletionResults(input));
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
            listener.onFailure(
                new ElasticsearchStatusException(
                    TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                    RestStatus.BAD_REQUEST
                )
            );
        }

        @Override
        public void chunkedInfer(
            Model model,
            String query,
            List<ChunkInferenceInput> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            TimeValue timeout,
            ActionListener<List<ChunkedInference>> listener
        ) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                    RestStatus.BAD_REQUEST
                )
            );
        }

        private InferenceServiceResults makeChatCompletionResults(List<String> inputs) {
            List<ChatCompletionResults.Result> results = new ArrayList<>();
            for (String text : inputs) {
                results.add(new ChatCompletionResults.Result(text.toUpperCase(Locale.ROOT)));
            }

            return new ChatCompletionResults(results);
        }

        public static class Configuration {
            public static InferenceServiceConfiguration get() {
                return configuration.getOrCompute();
            }

            private static final LazyInitializable<InferenceServiceConfiguration, RuntimeException> configuration = new LazyInitializable<>(
                () -> {
                    var configurationMap = new HashMap<String, SettingsConfiguration>();

                    configurationMap.put(
                        "model_id",
                        new SettingsConfiguration.Builder(EnumSet.of(TaskType.COMPLETION)).setDescription("")
                            .setLabel("Model ID")
                            .setRequired(true)
                            .setSensitive(true)
                            .setType(SettingsConfigurationFieldType.STRING)
                            .build()
                    );

                    return new InferenceServiceConfiguration.Builder().setService(NAME)
                        .setTaskTypes(SUPPORTED_TASK_TYPES)
                        .setConfigurations(configurationMap)
                        .build();
                }
            );
        }
    }

    public record TestServiceSettings(String modelId) implements ServiceSettings {
        public static final String NAME = "completion_test_service_settings";

        public TestServiceSettings(StreamInput in) throws IOException {
            this(in.readString());
        }

        public static TestServiceSettings fromMap(Map<String, Object> map) {
            var modelId = map.remove("model").toString();

            if (modelId == null) {
                ValidationException validationException = new ValidationException();
                validationException.addValidationError("missing model id");
                throw validationException;
            }

            return new TestServiceSettings(modelId);
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
            out.writeString(modelId());
        }

        @Override
        public ToXContentObject getFilteredXContentObject() {
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("model", modelId()).endObject();
        }
    }
}
