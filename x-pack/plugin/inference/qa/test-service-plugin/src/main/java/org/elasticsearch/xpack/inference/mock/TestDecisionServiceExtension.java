/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mock;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.DecisionQuestion;
import org.elasticsearch.inference.DecisionQuestions;
import org.elasticsearch.inference.DecisionRequest;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
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
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.results.DecisionResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.ModelConfigurations.TASK_SETTINGS;

public class TestDecisionServiceExtension implements InferenceServiceExtension {

    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new);
    }

    public static class TestInferenceService extends AbstractTestInferenceService {
        public static final String NAME = "test_decision_service";

        private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(TaskType.DECISION);

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
            var taskSettings = TestDecisionServiceExtension.TestTaskSettings.fromMap(taskSettingsMap, false);

            parsedModelListener.onResponse(new TestServiceModel(modelId, taskType, name(), serviceSettings, taskSettings, secretSettings));
        }

        @Override
        protected TaskSettings getTasksSettingsFromMap(Map<String, Object> taskSettingsMap) {
            return TestDecisionServiceExtension.TestTaskSettings.fromMap(taskSettingsMap, true);
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
            listener.onFailure(new UnsupportedOperationException("Decisions via infer() are not supported, use decisionInfer() instead"));
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
        public void decisionInfer(
            Model model,
            DecisionRequest request,
            TimeValue timeout,
            ActionListener<InferenceServiceResults> listener
        ) {
            if (model.getConfigurations().getTaskType() != TaskType.DECISION) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                        RestStatus.BAD_REQUEST
                    )
                );
                return;
            }

            try {
                var taskSettings = (TestDecisionServiceExtension.TestTaskSettings) model.getTaskSettings()
                    .updatedTaskSettings(request.taskSettings());
                if (taskSettings.shouldFailInference()) {
                    listener.onFailure(new RuntimeException("inference call intentionally failed based on task settings"));
                    return;
                }
                if (taskSettings.questions().isEmpty()) {
                    var validationException = new ValidationException();
                    validationException.addValidationError(
                        Strings.format(
                            "No questions to answer, specify [%s] in the [%s] of the inference endpoint or the request",
                            DecisionQuestions.QUESTIONS_FIELD,
                            TASK_SETTINGS
                        )
                    );
                    throw validationException;
                }

                listener.onResponse(makeResults(request.input().value(), taskSettings.questions()));
            } catch (Exception e) {
                listener.onFailure(e);
            }
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
         * Produces one answer per question. The answers are derived deterministically from the input and the question name, so tests can
         * assert that the same request produces the same results.
         */
        private static DecisionResults makeResults(String input, DecisionQuestions questions) {
            var answers = new LinkedHashMap<String, DecisionResults.Answer>();
            for (var question : questions.questions().entrySet()) {
                var weight = stringWeight(input + question.getKey(), 0);
                answers.put(question.getKey(), switch (question.getValue()) {
                    case DecisionQuestion.Predicate predicate -> new DecisionResults.PredicateAnswer((weight % 101) / 100.0);
                    case DecisionQuestion.Choice choice -> {
                        var options = new ArrayList<>(choice.choices().keySet());
                        var selected = weight % options.size();
                        var probabilities = probabilities(options, selected);
                        yield new DecisionResults.ChoiceAnswer(
                            options.get(selected),
                            probabilities,
                            probabilities.get(options.get(selected))
                        );
                    }
                    case DecisionQuestion.Score score -> {
                        var levels = score.labels();
                        var selected = weight % levels.size();
                        var probabilities = probabilities(levels, selected);
                        var weightedScore = 0.0;
                        for (int i = 0; i < levels.size(); i++) {
                            weightedScore += i * probabilities.get(levels.get(i));
                        }
                        yield new DecisionResults.ScoreAnswer(weightedScore, probabilities, probabilities.get(levels.get(selected)));
                    }
                });
            }
            return new DecisionResults(answers);
        }

        /**
         * Assigns most of the probability mass to the {@code selected} option and spreads the rest evenly over the other options.
         */
        private static Map<String, Double> probabilities(List<String> options, int selected) {
            var selectedProbability = 0.7;
            var otherProbability = (1.0 - selectedProbability) / (options.size() - 1);
            var probabilities = new LinkedHashMap<String, Double>();
            for (int i = 0; i < options.size(); i++) {
                probabilities.put(options.get(i), i == selected ? selectedProbability : otherProbability);
            }
            return probabilities;
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
                        new SettingsConfiguration.Builder(EnumSet.of(TaskType.DECISION)).setDescription("")
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

    /**
     * Task settings holding the stored {@link DecisionQuestions}. Questions specified in the request task settings replace the stored
     * questions as a whole.
     */
    public record TestTaskSettings(DecisionQuestions questions, boolean shouldFailInference) implements TaskSettings {

        static final String NAME = "test_decision_task_settings";
        static final String SHOULD_FAIL_INFERENCE = "should_fail_inference";

        static final TestTaskSettings EMPTY = new TestTaskSettings(DecisionQuestions.EMPTY, false);

        private static final ObjectParser<Builder, Void> REQUEST_PARSER = createParser(false);
        private static final ObjectParser<Builder, Void> PERSISTENT_PARSER = createParser(true);

        private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
            var parser = new ObjectParser<Builder, Void>(TASK_SETTINGS, ignoreUnknownFields, Builder::new);
            parser.declareObject(
                Builder::setQuestions,
                (p, c) -> DecisionQuestions.parse(p, ignoreUnknownFields),
                new ParseField(DecisionQuestions.QUESTIONS_FIELD)
            );
            parser.declareBoolean(Builder::setShouldFailInference, new ParseField(SHOULD_FAIL_INFERENCE));
            return parser;
        }

        public static TestTaskSettings fromMap(@Nullable Map<String, Object> map, boolean ignoreUnknownFields) {
            return parse(map, ignoreUnknownFields).build(EMPTY);
        }

        private static Builder parse(@Nullable Map<String, Object> map, boolean ignoreUnknownFields) {
            if (map == null || map.isEmpty()) {
                return new Builder();
            }

            var parser = ignoreUnknownFields ? PERSISTENT_PARSER : REQUEST_PARSER;
            try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
                return parser.apply(xParser, null);
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse [{}]", e, TASK_SETTINGS);
            }
        }

        public TestTaskSettings {
            Objects.requireNonNull(questions);
        }

        public TestTaskSettings(StreamInput in) throws IOException {
            this(new DecisionQuestions(in), in.readBoolean());
        }

        @Override
        public boolean isEmpty() {
            return questions.isEmpty() && shouldFailInference == false;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            questions.writeTo(out);
            out.writeBoolean(shouldFailInference);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (questions.isEmpty() == false) {
                builder.field(DecisionQuestions.QUESTIONS_FIELD, questions);
            }
            builder.field(SHOULD_FAIL_INFERENCE, shouldFailInference);
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
            return parse(newSettingsMap, false).build(this);
        }

        /**
         * Collects the parsed settings. Settings that are not set fall back to the ones of the settings they are applied to.
         */
        private static class Builder {
            private DecisionQuestions questions;
            private Boolean shouldFailInference;

            private void setQuestions(DecisionQuestions questions) {
                this.questions = questions;
            }

            private void setShouldFailInference(boolean shouldFailInference) {
                this.shouldFailInference = shouldFailInference;
            }

            private TestTaskSettings build(TestTaskSettings original) {
                return new TestTaskSettings(
                    Objects.requireNonNullElse(questions, original.questions()),
                    Objects.requireNonNullElse(shouldFailInference, original.shouldFailInference())
                );
            }
        }
    }

    public record TestServiceSettings(String modelId) implements ServiceSettings {

        static final String NAME = "test_decision_service_settings";

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
