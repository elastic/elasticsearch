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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.InferenceServicePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestInferenceServicePlugin extends Plugin implements InferenceServicePlugin {

    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new, TestInferenceServiceClusterService::new);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getInferenceServiceNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, TestServiceSettings.NAME, TestServiceSettings::new),
            new NamedWriteableRegistry.Entry(TaskSettings.class, TestTaskSettings.NAME, TestTaskSettings::new),
            new NamedWriteableRegistry.Entry(SecretSettings.class, TestSecretSettings.NAME, TestSecretSettings::new)
        );
    }

    public static class TestInferenceService extends TestInferenceServiceBase {
        private static final String NAME = "test_service";

        public TestInferenceService(InferenceServiceFactoryContext context) {
            super(context);
        }

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current(); // fine for these tests but will not work for cluster upgrade tests
        }
    }

    public static class TestInferenceServiceClusterService extends TestInferenceServiceBase {
        private static final String NAME = "test_service_in_cluster_service";

        public TestInferenceServiceClusterService(InferenceServiceFactoryContext context) {
            super(context);
        }

        @Override
        public boolean isInClusterService() {
            return true;
        }

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current(); // fine for these tests but will not work for cluster upgrade tests
        }
    }

    public abstract static class TestInferenceServiceBase implements InferenceService {

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

        public TestInferenceServiceBase(InferenceServicePlugin.InferenceServiceFactoryContext context) {

        }

        @Override
        @SuppressWarnings("unchecked")
        public TestServiceModel parseRequestConfig(
            String modelId,
            TaskType taskType,
            Map<String, Object> config,
            Set<String> platfromArchitectures
        ) {
            var serviceSettingsMap = (Map<String, Object>) config.remove(ModelConfigurations.SERVICE_SETTINGS);
            var serviceSettings = TestServiceSettings.fromMap(serviceSettingsMap);
            var secretSettings = TestSecretSettings.fromMap(serviceSettingsMap);

            var taskSettingsMap = getTaskSettingsMap(config);
            var taskSettings = TestTaskSettings.fromMap(taskSettingsMap);

            return new TestServiceModel(modelId, taskType, name(), serviceSettings, taskSettings, secretSettings);
        }

        @Override
        @SuppressWarnings("unchecked")
        public TestServiceModel parsePersistedConfig(
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
        public void infer(
            Model model,
            List<String> input,
            Map<String, Object> taskSettings,
            ActionListener<InferenceServiceResults> listener
        ) {
            switch (model.getConfigurations().getTaskType()) {
                case ANY -> listener.onResponse(makeResults(input));
                case SPARSE_EMBEDDING -> listener.onResponse(makeResults(input));
                default -> listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }

        private TestResults makeResults(List<String> input) {
            var strings = new ArrayList<String>();
            for (int i = 0; i < input.size(); i++) {
                strings.add(Integer.toString(i));
            }
            return new TestResults(strings);
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

    public record TestServiceSettings(String model) implements ServiceSettings {

        private static final String NAME = "test_service_settings";

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
            return TransportVersion.current(); // fine for these tests but will not work for cluster upgrade tests
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(model);
        }
    }

    public record TestTaskSettings(Integer temperature) implements TaskSettings {

        private static final String NAME = "test_task_settings";

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

        private static final String NAME = "test_secret_settings";

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

    private static class TestResults implements InferenceServiceResults, InferenceResults {

        private static final String RESULTS_FIELD = "result";
        private List<String> result;

        TestResults(List<String> result) {
            this.result = result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(RESULTS_FIELD, result);
            return builder;
        }

        @Override
        public String getWriteableName() {
            return "test_result";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(result);
        }

        @Override
        public String getResultsField() {
            return RESULTS_FIELD;
        }

        @Override
        public List<? extends InferenceResults> transformToLegacyFormat() {
            return List.of(this);
        }

        @Override
        public Map<String, Object> asMap() {
            var map = new HashMap<String, Object>();
            map.put("result", result);
            return map;
        }

        @Override
        public Map<String, Object> asMap(String outputField) {
            var map = new HashMap<String, Object>();
            map.put(outputField, result);
            return map;
        }

        @Override
        public Object predictedValue() {
            return result;
        }
    }
}
