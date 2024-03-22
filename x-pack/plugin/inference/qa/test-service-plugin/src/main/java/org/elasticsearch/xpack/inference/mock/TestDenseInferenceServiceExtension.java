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
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestDenseInferenceServiceExtension implements InferenceServiceExtension {
    @Override
    public List<Factory> getInferenceServiceFactories() {
        return List.of(TestInferenceService::new);
    }

    public static class TestInferenceService extends AbstractTestInferenceService {
        private static final String NAME = "text_embedding_test_service";

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
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            ActionListener<InferenceServiceResults> listener
        ) {
            switch (model.getConfigurations().getTaskType()) {
                case ANY, TEXT_EMBEDDING -> listener.onResponse(
                    makeResults(input, ((TestServiceModel) model).getServiceSettings().dimensions())
                );
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
                case ANY, TEXT_EMBEDDING -> listener.onResponse(
                    makeChunkedResults(input, ((TestServiceModel) model).getServiceSettings().dimensions())
                );
                default -> listener.onFailure(
                    new ElasticsearchStatusException(
                        TaskType.unsupportedTaskTypeErrorMsg(model.getConfigurations().getTaskType(), name()),
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }

        private TextEmbeddingResults makeResults(List<String> input, int dimensions) {
            List<TextEmbeddingResults.Embedding> embeddings = new ArrayList<>();
            for (int i = 0; i < input.size(); i++) {
                List<Float> values = new ArrayList<>();
                for (int j = 0; j < dimensions; j++) {
                    values.add((float) j);
                }
                embeddings.add(new TextEmbeddingResults.Embedding(values));
            }
            return new TextEmbeddingResults(embeddings);
        }

        private List<ChunkedInferenceServiceResults> makeChunkedResults(List<String> input, int dimensions) {
            var results = new ArrayList<ChunkedInferenceServiceResults>();
            for (int i = 0; i < input.size(); i++) {
                double[] values = new double[dimensions];
                for (int j = 0; j < 5; j++) {
                    values[j] = j;
                }
                results.add(
                    new org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingResults(
                        List.of(new ChunkedTextEmbeddingResults.EmbeddingChunk(input.get(i), values))
                    )
                );
            }
            return results;
        }

        protected ServiceSettings getServiceSettingsFromMap(Map<String, Object> serviceSettingsMap) {
            return TestServiceSettings.fromMap(serviceSettingsMap);
        }
    }

    public record TestServiceSettings(String model, Integer dimensions, SimilarityMeasure similarity) implements ServiceSettings {

        static final String NAME = "test_text_embedding_service_settings";

        public static TestServiceSettings fromMap(Map<String, Object> map) {
            ValidationException validationException = new ValidationException();

            String model = (String) map.remove("model");
            if (model == null) {
                validationException.addValidationError("missing model");
            }

            Integer dimensions = (Integer) map.remove("dimensions");
            if (dimensions == null) {
                validationException.addValidationError("missing dimensions");
            }

            SimilarityMeasure similarity = null;
            String similarityStr = (String) map.remove("similarity");
            if (similarityStr != null) {
                similarity = SimilarityMeasure.fromString(similarityStr);
            }

            return new TestServiceSettings(model, dimensions, similarity);
        }

        public TestServiceSettings(StreamInput in) throws IOException {
            this(in.readString(), in.readOptionalInt(), in.readOptionalEnum(SimilarityMeasure.class));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("model", model);
            builder.field("dimensions", dimensions);
            if (similarity != null) {
                builder.field("similarity", similarity);
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
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(model);
            out.writeInt(dimensions);
            out.writeOptionalEnum(similarity);
        }

        @Override
        public ToXContentObject getFilteredXContentObject() {
            return (builder, params) -> {
                builder.startObject();
                builder.field("model", model);
                builder.field("dimensions", dimensions);
                if (similarity != null) {
                    builder.field("similarity", similarity);
                }
                builder.endObject();
                return builder;
            };
        }

    }

}
