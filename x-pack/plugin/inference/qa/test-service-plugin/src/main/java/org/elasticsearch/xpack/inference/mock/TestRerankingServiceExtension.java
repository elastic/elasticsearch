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
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.TopNProvider;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mock.AbstractTestInferenceService.random;

public class TestRerankingServiceExtension implements InferenceServiceExtension {

    public static final int RERANK_WINDOW_SIZE = 333;

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

    public static class TestInferenceService extends AbstractTestInferenceService implements RerankingInferenceService {
        public static final String NAME = "test_reranking_service";

        private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(TaskType.RERANK);

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
            var taskSettings = TestRerankingServiceExtension.TestTaskSettings.fromMap(taskSettingsMap);

            parsedModelListener.onResponse(new TestServiceModel(modelId, taskType, name(), serviceSettings, taskSettings, secretSettings));
        }

        protected TaskSettings getTasksSettingsFromMap(Map<String, Object> taskSettingsMap) {
            return TestRerankingServiceExtension.TestTaskSettings.fromMap(taskSettingsMap);
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
            @Nullable Boolean returnDocuments,
            @Nullable Integer topN,
            List<String> input,
            boolean stream,
            Map<String, Object> taskSettingsMap,
            InputType inputType,
            TimeValue timeout,
            ActionListener<InferenceServiceResults> listener
        ) {
            if (((TestRerankingServiceExtension.TestTaskSettings) model.getTaskSettings()).shouldFailValidation()) {
                listener.onFailure(new RuntimeException("validation call intentionally failed based on task settings"));
                return;
            }
            TaskSettings taskSettings = model.getTaskSettings().updatedTaskSettings(taskSettingsMap);

            switch (model.getConfigurations().getTaskType()) {
                case ANY, RERANK -> listener.onResponse(makeResults(input, (TestRerankingServiceExtension.TestTaskSettings) taskSettings));
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
        public void chunkedInfer(
            Model model,
            @Nullable String query,
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

        private RankedDocsResults makeResults(List<String> input, TestRerankingServiceExtension.TestTaskSettings taskSettings) {
            if (taskSettings.useTextLength) {
                return makeResultFromTextInput(input, taskSettings);
            }

            try {
                int totalResults = input.size();
                List<RankedDocsResults.RankedDoc> results = new ArrayList<>();
                for (int i = 0; i < totalResults; i++) {
                    results.add(new RankedDocsResults.RankedDoc(i, Float.parseFloat(input.get(i)), input.get(i)));
                }

                // RankedDoc's compareTo implementation already sorts by score descending, so we don't need to reverse the sort order
                var sortedResultsStream = results.stream().sorted();
                if (taskSettings.topN != null) {
                    sortedResultsStream = sortedResultsStream.limit(taskSettings.topN);
                }

                return new RankedDocsResults(sortedResultsStream.toList());
            } catch (NumberFormatException ex) {
                return makeResultFromTextInput(input, taskSettings);
            }
        }

        private RankedDocsResults makeResultFromTextInput(List<String> input, TestRerankingServiceExtension.TestTaskSettings taskSettings) {
            int totalResults = input.size();

            List<RankedDocsResults.RankedDoc> results = new ArrayList<>();
            float minScore = taskSettings.minScore();
            float resultDiff = taskSettings.resultDiff();
            for (int i = 0; i < input.size(); i++) {
                float relevanceScore = minScore + resultDiff * (totalResults - i);
                String inputText = input.get(totalResults - 1 - i);
                if (taskSettings.useTextLength()) {
                    relevanceScore = 1f / inputText.length();
                }
                results.add(new RankedDocsResults.RankedDoc(totalResults - 1 - i, relevanceScore, inputText));
            }
            // Ensure result are sorted by descending score
            results.sort((a, b) -> -Float.compare(a.relevanceScore(), b.relevanceScore()));
            if (taskSettings.topN != null && taskSettings.topN < results.size()) {
                results = results.subList(0, taskSettings.topN);
            }

            return new RankedDocsResults(results);
        }

        protected ServiceSettings getServiceSettingsFromMap(Map<String, Object> serviceSettingsMap) {
            return TestServiceSettings.fromMap(serviceSettingsMap);
        }

        @Override
        public int rerankerWindowSize(String modelId) {
            return RERANK_WINDOW_SIZE;
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
                        new SettingsConfiguration.Builder(EnumSet.of(TaskType.RERANK)).setDescription("")
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

    public record TestTaskSettings(
        boolean shouldFailValidation,
        boolean useTextLength,
        float minScore,
        float resultDiff,
        Integer topN,
        boolean hideTopN
    ) implements TaskSettings, TopNProvider {

        static final String NAME = "test_reranking_task_settings";

        public static TestTaskSettings fromMap(Map<String, Object> map) {
            boolean shouldFailValidation = false;
            boolean useTextLength = false;
            float minScore = random.nextFloat(-1f, 1f);
            float resultDiff = 0.2f;
            Integer topN = null;
            boolean hideTopN = false;

            if (map.containsKey("should_fail_validation")) {
                shouldFailValidation = Boolean.parseBoolean(map.remove("should_fail_validation").toString());
            }

            if (map.containsKey("use_text_length")) {
                useTextLength = Boolean.parseBoolean(map.remove("use_text_length").toString());
            }

            if (map.containsKey("min_score")) {
                minScore = Float.parseFloat(map.remove("min_score").toString());
            }

            if (map.containsKey("result_diff")) {
                resultDiff = Float.parseFloat(map.remove("result_diff").toString());
            }

            if (map.containsKey("top_n")) {
                topN = Integer.parseInt(map.remove("top_n").toString());
            }

            if (map.containsKey("hide_top_n")) {
                hideTopN = Boolean.parseBoolean(map.remove("hide_top_n").toString());
            }

            return new TestTaskSettings(shouldFailValidation, useTextLength, minScore, resultDiff, topN, hideTopN);
        }

        public TestTaskSettings(StreamInput in) throws IOException {
            this(in.readBoolean(), in.readBoolean(), in.readFloat(), in.readFloat(), in.readOptionalInt(), in.readBoolean());
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(shouldFailValidation);
            out.writeBoolean(useTextLength);
            out.writeFloat(minScore);
            out.writeFloat(resultDiff);
            out.writeOptionalInt(topN);
            out.writeBoolean(hideTopN);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("should_fail_validation", shouldFailValidation);
            builder.field("use_text_length", useTextLength);
            builder.field("min_score", minScore);
            builder.field("result_diff", resultDiff);
            if (topN != null) {
                builder.field("top_n", topN);
            }
            builder.field("hide_top_n", hideTopN);
            builder.endObject();
            return builder;
        }

        @Override
        public Integer getTopN() {
            return hideTopN ? null : topN;
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
            TestTaskSettings newSettingsObject = fromMap(new HashMap<>(newSettingsMap));
            return new TestTaskSettings(
                newSettingsMap.containsKey("should_fail_validation") ? newSettingsObject.shouldFailValidation() : shouldFailValidation,
                newSettingsMap.containsKey("use_text_length") ? newSettingsObject.useTextLength() : useTextLength,
                newSettingsMap.containsKey("min_score") ? newSettingsObject.minScore() : minScore,
                newSettingsMap.containsKey("result_diff") ? newSettingsObject.resultDiff() : resultDiff,
                newSettingsMap.containsKey("top_n") ? newSettingsObject.topN() : topN,
                newSettingsMap.containsKey("hide_top_n") ? newSettingsObject.hideTopN() : hideTopN
            );
        }
    }

    public record TestServiceSettings(String modelId) implements ServiceSettings {

        static final String NAME = "test_reranking_service_settings";

        public static TestServiceSettings fromMap(Map<String, Object> map) {
            ValidationException validationException = new ValidationException();

            String model = (String) map.remove("model_id");

            if (model == null) {
                model = (String) map.remove("model");
                if (model == null) {
                    validationException.addValidationError("missing model");
                }
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
