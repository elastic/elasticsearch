/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mock;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
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
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.DequeUtils;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResults.COMPLETION;

public class TestStreamingCompletionServiceExtension implements InferenceServiceExtension {
    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new);
    }

    public static class TestInferenceService extends AbstractTestInferenceService {
        private static final String NAME = "streaming_completion_test_service";
        private static final String ALIAS = "streaming_completion_test_service_alias";
        private static final Set<TaskType> supportedStreamingTasks = Set.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);

        private static final EnumSet<TaskType> supportedTaskTypes = EnumSet.of(
            TaskType.COMPLETION,
            TaskType.CHAT_COMPLETION,
            TaskType.SPARSE_EMBEDDING
        );

        public TestInferenceService(InferenceServiceExtension.InferenceServiceFactoryContext context) {}

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public List<String> aliases() {
            return List.of(ALIAS);
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
            return supportedTaskTypes;
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
                case SPARSE_EMBEDDING -> {
                    if (stream) {
                        listener.onFailure(
                            new ElasticsearchStatusException(
                                TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                                RestStatus.BAD_REQUEST
                            )
                        );
                    } else {
                        // Return text embedding results when creating a sparse_embedding inference endpoint to allow creation validation to
                        // pass. This is required to test that streaming fails for a sparse_embedding endpoint.
                        listener.onResponse(makeTextEmbeddingResults(input));
                    }
                }
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
            switch (model.getConfigurations().getTaskType()) {
                case CHAT_COMPLETION -> listener.onResponse(makeUnifiedResults(request));
                default -> listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }

        private StreamingChatCompletionResults makeChatCompletionResults(List<String> input) {
            var responseIter = input.stream().map(s -> s.toUpperCase(Locale.ROOT)).iterator();
            return new StreamingChatCompletionResults(subscriber -> {
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long n) {
                        if (responseIter.hasNext()) {
                            subscriber.onNext(completionChunk(responseIter.next()));
                        } else {
                            subscriber.onComplete();
                        }
                    }

                    @Override
                    public void cancel() {}
                });
            });
        }

        private TextEmbeddingFloatResults makeTextEmbeddingResults(List<String> input) {
            var embeddings = new ArrayList<TextEmbeddingFloatResults.Embedding>();
            for (int i = 0; i < input.size(); i++) {
                var values = new float[5];
                for (int j = 0; j < 5; j++) {
                    values[j] = random.nextFloat();
                }
                embeddings.add(new TextEmbeddingFloatResults.Embedding(values));
            }
            return new TextEmbeddingFloatResults(embeddings);
        }

        private InferenceServiceResults.Result completionChunk(String delta) {
            return new InferenceServiceResults.Result() {
                @Override
                public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                    return ChunkedToXContentHelper.chunk(
                        (b, p) -> b.startObject()
                            .startArray(COMPLETION)
                            .startObject()
                            .field("delta", delta)
                            .endObject()
                            .endArray()
                            .endObject()
                    );
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeString(delta);
                }

                @Override
                public String getWriteableName() {
                    return "test_completionChunk";
                }
            };
        }

        private StreamingUnifiedChatCompletionResults makeUnifiedResults(UnifiedCompletionRequest request) {
            var responseIter = request.messages().stream().map(message -> message.content().toString().toUpperCase(Locale.ROOT)).iterator();
            return new StreamingUnifiedChatCompletionResults(subscriber -> {
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long n) {
                        if (responseIter.hasNext()) {
                            subscriber.onNext(unifiedCompletionChunk(responseIter.next()));
                        } else {
                            subscriber.onComplete();
                        }
                    }

                    @Override
                    public void cancel() {}
                });
            });
        }

        /*
        The response format looks like this
        {
          "id": "chatcmpl-AarrzyuRflye7yzDF4lmVnenGmQCF",
          "choices": [
            {
              "delta": {
                "content": " information"
              },
              "index": 0
            }
          ],
          "model": "gpt-4o-2024-08-06",
          "object": "chat.completion.chunk"
        }
         */
        private StreamingUnifiedChatCompletionResults.Results unifiedCompletionChunk(String delta) {
            return new StreamingUnifiedChatCompletionResults.Results(
                DequeUtils.of(
                    new StreamingUnifiedChatCompletionResults.ChatCompletionChunk(
                        "id",
                        List.of(
                            new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice(
                                new StreamingUnifiedChatCompletionResults.ChatCompletionChunk.Choice.Delta(delta, null, null, null),
                                null,
                                0
                            )
                        ),
                        "gpt-4o-2024-08-06",
                        "chat.completion.chunk",
                        null
                    )
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

        @Override
        public Set<TaskType> supportedStreamingTasks() {
            return supportedStreamingTasks;
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
                        .setTaskTypes(supportedTaskTypes)
                        .setConfigurations(configurationMap)
                        .build();
                }
            );
        }
    }

    public record TestServiceSettings(String modelId) implements ServiceSettings {
        public static final String NAME = "streaming_completion_test_service_settings";

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
