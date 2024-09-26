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
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestRerankingServiceExtension implements InferenceServiceExtension {
    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new);
    }

    public static class TestRerankingModel extends Model {
        public TestRerankingModel(String inferenceEntityId, TestServiceSettings serviceSettings) {
            super(
                new ModelConfigurations(inferenceEntityId, TaskType.RERANK, TestInferenceService.NAME, serviceSettings),
                new ModelSecrets(new AbstractTestInferenceService.TestSecretSettings("api_key"))
            );
        }
    }

    public static class TestInferenceService extends AbstractTestInferenceService {
        public static final String NAME = "test_reranking_service";

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
            Set<String> platformArchitectures,
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
                case ANY, RERANK -> listener.onResponse(makeResults(input));
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
            listener.onFailure(
                new ElasticsearchStatusException(
                    TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                    RestStatus.BAD_REQUEST
                )
            );
        }

        private RankedDocsResults makeResults(List<String> input) {
            List<RankedDocsResults.RankedDoc> results = new ArrayList<>();
            int totalResults = input.size();
            float resultDiff = 0.2f;
            for (int i = 0; i < input.size(); i++) {
                results.add(new RankedDocsResults.RankedDoc(totalResults - 1 - i, resultDiff * (totalResults - i), input.get(i)));
            }
            return new RankedDocsResults(results);
        }

        protected ServiceSettings getServiceSettingsFromMap(Map<String, Object> serviceSettingsMap) {
            return TestServiceSettings.fromMap(serviceSettingsMap);
        }
    }

    public record TestServiceSettings(String modelId) implements ServiceSettings {

        static final String NAME = "test_reranking_service_settings";

        public static TestServiceSettings fromMap(Map<String, Object> map) {
            ValidationException validationException = new ValidationException();

            String model = (String) map.remove("model_id");

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
