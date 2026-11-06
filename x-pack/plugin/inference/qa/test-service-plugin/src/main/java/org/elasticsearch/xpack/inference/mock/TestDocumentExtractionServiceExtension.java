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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.DocumentExtractionRequest;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.DocumentExtractionResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestDocumentExtractionServiceExtension implements InferenceServiceExtension {

    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new);
    }

    public static class TestInferenceService extends AbstractTestInferenceService {
        public static final String NAME = "test_document_extraction_service";

        private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(TaskType.DOCUMENT_EXTRACTION);

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
            var taskSettings = TestDocumentExtractionServiceExtension.TestTaskSettings.fromMap(taskSettingsMap);

            parsedModelListener.onResponse(new TestServiceModel(modelId, taskType, name(), serviceSettings, taskSettings, secretSettings));
        }

        @Override
        protected TaskSettings getTasksSettingsFromMap(Map<String, Object> taskSettingsMap) {
            return TestDocumentExtractionServiceExtension.TestTaskSettings.fromMap(taskSettingsMap);
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
            List<String> input,
            boolean stream,
            Map<String, Object> taskSettingsMap,
            InputType inputType,
            TimeValue timeout,
            ActionListener<InferenceServiceResults> listener
        ) {
            listener.onFailure(
                new UnsupportedOperationException("Document extraction via infer() is not supported, use documentExtractionInfer() instead")
            );
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
        public void embeddingInfer(
            Model model,
            EmbeddingRequest request,
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
        public void rerankInfer(Model model, RerankRequest request, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                    RestStatus.BAD_REQUEST
                )
            );
        }

        @Override
        public void documentExtractionInfer(
            Model model,
            DocumentExtractionRequest request,
            TimeValue timeout,
            ActionListener<InferenceServiceResults> listener
        ) {
            if (model.getConfigurations().getTaskType() != TaskType.DOCUMENT_EXTRACTION) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                        RestStatus.BAD_REQUEST
                    )
                );
                return;
            }

            var taskSettings = (TestDocumentExtractionServiceExtension.TestTaskSettings) model.getTaskSettings()
                .updatedTaskSettings(request.taskSettings());
            if (taskSettings.shouldFailInference()) {
                listener.onFailure(new RuntimeException("inference call intentionally failed based on task settings"));
                return;
            }

            listener.onResponse(makeResults(request.inputs(), taskSettings));
        }

        @Override
        public void chunkedInfer(
            Model model,
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

        /**
         * Produces one result per input document. The extracted content is derived deterministically from the input's declared media
         * type and base64 payload so tests can assert that different inputs produce different results.
         */
        private DocumentExtractionResults makeResults(
            List<InferenceString> inputs,
            TestDocumentExtractionServiceExtension.TestTaskSettings taskSettings
        ) {
            var results = new ArrayList<DocumentExtractionResults.Result>();
            for (int i = 0; i < inputs.size(); i++) {
                var input = inputs.get(i);
                var dataUri = InferenceString.tryParseDataUri(input.value());
                var mediaType = dataUri == null ? "unknown" : dataUri.mediaType();
                var payloadWeight = dataUri == null ? stringWeight(input.value(), i) : stringWeight(dataUri.base64Data(), i);
                var content = Strings.format("extracted content [%d] of [%s] document at index [%d]", payloadWeight, mediaType, i);
                results.add(new DocumentExtractionResults.Result(content, taskSettings.outputFormat(), Map.of("media_type", mediaType)));
            }
            return new DocumentExtractionResults(results);
        }

        @Override
        protected ServiceSettings getServiceSettingsFromMap(Map<String, Object> serviceSettingsMap) {
            return TestServiceSettings.fromMap(serviceSettingsMap);
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
                        new SettingsConfiguration.Builder(EnumSet.of(TaskType.DOCUMENT_EXTRACTION)).setDescription("")
                            .setLabel("Model")
                            .setRequired(true)
                            .setSensitive(true)
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

    public record TestTaskSettings(String outputFormat, boolean shouldFailInference) implements TaskSettings {

        static final String NAME = "test_document_extraction_task_settings";
        static final String DEFAULT_OUTPUT_FORMAT = "markdown";

        private record OptionalTaskSettings(String outputFormat, Boolean shouldFailInference) {}

        public static TestTaskSettings fromMap(Map<String, Object> map) {
            var optionalSettings = parseAsOptional(map);
            return new TestTaskSettings(
                Objects.requireNonNullElse(optionalSettings.outputFormat, DEFAULT_OUTPUT_FORMAT),
                Objects.requireNonNullElse(optionalSettings.shouldFailInference, false)
            );
        }

        private static OptionalTaskSettings parseAsOptional(Map<String, Object> map) {
            String outputFormat = null;
            Boolean shouldFailInference = null;

            if (map.containsKey("output_format")) {
                outputFormat = map.remove("output_format").toString();
            }

            if (map.containsKey("should_fail_inference")) {
                shouldFailInference = Boolean.parseBoolean(map.remove("should_fail_inference").toString());
            }

            return new OptionalTaskSettings(outputFormat, shouldFailInference);
        }

        public TestTaskSettings(StreamInput in) throws IOException {
            this(in.readString(), in.readBoolean());
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(outputFormat);
            out.writeBoolean(shouldFailInference);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("output_format", outputFormat);
            builder.field("should_fail_inference", shouldFailInference);
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
        public TaskSettings updatedTaskSettings(Map<String, Object> newSettingsMap) {
            var optionalTaskSettings = parseAsOptional(newSettingsMap);
            return new TestTaskSettings(
                Objects.requireNonNullElse(optionalTaskSettings.outputFormat(), outputFormat),
                Objects.requireNonNullElse(optionalTaskSettings.shouldFailInference(), shouldFailInference)
            );
        }
    }

    public record TestServiceSettings(String modelId) implements ServiceSettings {

        static final String NAME = "test_document_extraction_service_settings";

        public static TestServiceSettings fromMap(Map<String, Object> map) {
            ValidationException validationException = new ValidationException();

            String model = (String) map.remove("model_id");

            if (model == null) {
                model = (String) map.remove("model");
                if (model == null) {
                    validationException.addValidationError("missing model");
                }
            }

            validationException.throwIfValidationErrorsExist();

            return new TestServiceSettings(model);
        }

        public TestServiceSettings(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("model_id", modelId);
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
            out.writeString(modelId);
        }

        @Override
        public String modelId() {
            return modelId;
        }

        @Override
        public ToXContentObject getFilteredXContentObject() {
            return (builder, params) -> {
                builder.startObject();
                builder.field("model_id", modelId);
                builder.endObject();
                return builder;
            };
        }
    }
}
