/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.InferenceServicePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResultsTests;
import org.elasticsearch.xpack.inference.services.MapParsingUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.MapParsingUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.throwIfNotEmptyMap;

public class TestInferenceServicePlugin extends Plugin implements InferenceServicePlugin {

    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getInferenceServiceNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, TestServiceSettings.NAME, TestServiceSettings::new),
            new NamedWriteableRegistry.Entry(TaskSettings.class, TestTaskSettings.NAME, TestTaskSettings::new)
        );
    }

    public class TestInferenceService implements InferenceService {

        private static final String NAME = "test_service";

        public static TestServiceModel parseConfig(
            boolean throwOnUnknownFields,
            String modelId,
            TaskType taskType,
            Map<String, Object> settings
        ) {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(settings, Model.SERVICE_SETTINGS);
            var serviceSettings = TestServiceSettings.fromMap(serviceSettingsMap);

            Map<String, Object> taskSettingsMap;
            // task settings are optional
            if (settings.containsKey(Model.TASK_SETTINGS)) {
                taskSettingsMap = removeFromMapOrThrowIfNull(settings, Model.TASK_SETTINGS);
            } else {
                taskSettingsMap = Map.of();
            }

            var taskSettings = TestTaskSettings.fromMap(taskSettingsMap);

            if (throwOnUnknownFields) {
                throwIfNotEmptyMap(settings, NAME);
                throwIfNotEmptyMap(serviceSettingsMap, NAME);
                throwIfNotEmptyMap(taskSettingsMap, NAME);
            }

            return new TestServiceModel(modelId, taskType, NAME, serviceSettings, taskSettings);
        }

        public TestInferenceService(InferenceServicePlugin.InferenceServiceFactoryContext context) {

        }

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public TestServiceModel parseConfigStrict(String modelId, TaskType taskType, Map<String, Object> config) {
            return parseConfig(true, modelId, taskType, config);
        }

        @Override
        public TestServiceModel parseConfigLenient(String modelId, TaskType taskType, Map<String, Object> config) {
            return parseConfig(false, modelId, taskType, config);
        }

        @Override
        public void infer(Model model, String input, Map<String, Object> taskSettings, ActionListener<InferenceResults> listener) {
            switch (model.getTaskType()) {
                case SPARSE_EMBEDDING -> listener.onResponse(TextExpansionResultsTests.createRandomResults(1, 10));
                default -> listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getTaskType(), NAME),
                        RestStatus.BAD_REQUEST
                    )
                );
            }

        }

        @Override
        public void start(Model model, ActionListener<Boolean> listener) {
            listener.onResponse(true);
        }
    }

    public static class TestServiceModel extends Model {

        public TestServiceModel(
            String modelId,
            TaskType taskType,
            String service,
            TestServiceSettings serviceSettings,
            TestTaskSettings taskSettings
        ) {
            super(modelId, taskType, service, serviceSettings, taskSettings);
        }

        @Override
        public TestServiceSettings getServiceSettings() {
            return (TestServiceSettings) super.getServiceSettings();
        }

        @Override
        public TestTaskSettings getTaskSettings() {
            return (TestTaskSettings) super.getTaskSettings();
        }
    }

    public record TestServiceSettings(String model, String apiKey) implements ServiceSettings {

        private static final String NAME = "test_service_settings";

        public static TestServiceSettings fromMap(Map<String, Object> map) {
            ValidationException validationException = new ValidationException();

            String model = MapParsingUtils.removeAsType(map, "model", String.class);
            String apiKey = MapParsingUtils.removeAsType(map, "api_key", String.class);

            if (model == null) {
                validationException.addValidationError(MapParsingUtils.missingSettingErrorMsg("model", Model.SERVICE_SETTINGS));
            }
            if (apiKey == null) {
                validationException.addValidationError(MapParsingUtils.missingSettingErrorMsg("api_key", Model.SERVICE_SETTINGS));
            }

            if (validationException.validationErrors().isEmpty() == false) {
                throw validationException;
            }

            return new TestServiceSettings(model, apiKey);
        }

        public TestServiceSettings(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("model", model);
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(model);
            out.writeString(apiKey);
        }
    }

    public record TestTaskSettings(Integer temperature) implements TaskSettings {

        private static final String NAME = "test_task_settings";

        public static TestTaskSettings fromMap(Map<String, Object> map) {
            Integer temperature = MapParsingUtils.removeAsType(map, "temperature", Integer.class);
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
}
