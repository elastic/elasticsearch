/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mock;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractTestInferenceService implements InferenceService {

    protected static int stringWeight(String input, int position) {
        int hashCode = input.hashCode();
        if (hashCode < 0) {
            hashCode = -hashCode;
        }
        return hashCode + position;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current(); // fine for these tests but will not work for cluster upgrade tests
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> getTaskSettingsMap(Map<String, Object> settings) {
        Map<String, Object> taskSettingsMap;
        // task settings are optional
        if (settings.containsKey(ModelConfigurations.TASK_SETTINGS)) {
            taskSettingsMap = (Map<String, Object>) settings.remove(ModelConfigurations.TASK_SETTINGS);
        } else {
            taskSettingsMap = new HashMap<>();
        }

        return taskSettingsMap;
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

        var serviceSettings = getServiceSettingsFromMap(serviceSettingsMap);
        var secretSettings = TestSecretSettings.fromMap(secretSettingsMap);

        var taskSettingsMap = getTaskSettingsMap(config);
        var taskSettings = TestTaskSettings.fromMap(taskSettingsMap);

        return new TestServiceModel(modelId, taskType, name(), serviceSettings, taskSettings, secretSettings);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        var serviceSettingsMap = (Map<String, Object>) config.remove(ModelConfigurations.SERVICE_SETTINGS);

        var serviceSettings = getServiceSettingsFromMap(serviceSettingsMap);

        var taskSettingsMap = getTaskSettingsMap(config);
        var taskSettings = TestTaskSettings.fromMap(taskSettingsMap);

        return new TestServiceModel(modelId, taskType, name(), serviceSettings, taskSettings, null);
    }

    protected abstract ServiceSettings getServiceSettingsFromMap(Map<String, Object> serviceSettingsMap);

    @Override
    public void start(Model model, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public void close() throws IOException {}

    public static class TestServiceModel extends Model {

        public TestServiceModel(
            String modelId,
            TaskType taskType,
            String service,
            ServiceSettings serviceSettings,
            TestTaskSettings taskSettings,
            TestSecretSettings secretSettings
        ) {
            super(new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secretSettings));
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
        public boolean isEmpty() {
            return temperature == null;
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

        @Override
        public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
            return fromMap(new HashMap<>(newSettings));
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

        @Override
        public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
            return TestSecretSettings.fromMap(new HashMap<>(newSecrets));
        }
    }
}
