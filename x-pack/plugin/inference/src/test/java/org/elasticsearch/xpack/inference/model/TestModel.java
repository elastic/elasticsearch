/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.model;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;

public class TestModel extends Model {

    public static TestModel createRandomInstance() {
        return createRandomInstance(randomFrom(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING));
    }

    public static TestModel createRandomInstance(TaskType taskType) {
        var dimensions = taskType == TaskType.TEXT_EMBEDDING ? randomInt(64) : null;
        var similarity = taskType == TaskType.TEXT_EMBEDDING ? randomFrom(SimilarityMeasure.values()) : null;
        var elementType = taskType == TaskType.TEXT_EMBEDDING ? randomFrom(DenseVectorFieldMapper.ElementType.values()) : null;
        return new TestModel(
            randomAlphaOfLength(4),
            taskType,
            randomAlphaOfLength(10),
            new TestModel.TestServiceSettings(randomAlphaOfLength(4), dimensions, similarity, elementType),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(randomAlphaOfLength(4))
        );
    }

    public TestModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        TestServiceSettings serviceSettings,
        TestTaskSettings taskSettings,
        TestSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings)
        );
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

    public record TestServiceSettings(
        String model,
        Integer dimensions,
        SimilarityMeasure similarity,
        DenseVectorFieldMapper.ElementType elementType
    ) implements ServiceSettings {

        private static final String NAME = "test_service_settings";

        public static TestServiceSettings fromMap(Map<String, Object> map) {
            ValidationException validationException = new ValidationException();

            String model = ServiceUtils.removeAsType(map, "model", String.class);

            if (model == null) {
                validationException.addValidationError(ServiceUtils.missingSettingErrorMsg("model", ModelConfigurations.SERVICE_SETTINGS));
            }

            if (validationException.validationErrors().isEmpty() == false) {
                throw validationException;
            }

            return new TestServiceSettings(model, null, null, null);
        }

        public TestServiceSettings(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readOptionalVInt(),
                in.readOptionalEnum(SimilarityMeasure.class),
                in.readOptionalEnum(DenseVectorFieldMapper.ElementType.class)
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("model", model);
            if (dimensions != null) {
                builder.field("dimensions", dimensions());
            }
            if (similarity != null) {
                builder.field("similarity", similarity);
            }
            if (elementType != null) {
                builder.field("element_type", elementType);
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
            out.writeOptionalVInt(dimensions);
            out.writeOptionalEnum(similarity);
            out.writeOptionalEnum(elementType);
        }

        @Override
        public ToXContentObject getFilteredXContentObject() {
            return this;
        }

        @Override
        public SimilarityMeasure similarity() {
            return similarity;
        }

        @Override
        public Integer dimensions() {
            return dimensions;
        }

        @Override
        public DenseVectorFieldMapper.ElementType elementType() {
            return elementType;
        }

        @Override
        public String modelId() {
            return model;
        }
    }

    public record TestTaskSettings(Integer temperature) implements TaskSettings {

        private static final String NAME = "test_task_settings";

        public static TestTaskSettings fromMap(Map<String, Object> map) {
            Integer temperature = ServiceUtils.removeAsType(map, "temperature", Integer.class);
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

            String apiKey = ServiceUtils.removeAsType(map, "api_key", String.class);

            if (apiKey == null) {
                validationException.addValidationError(ServiceUtils.missingSettingErrorMsg("api_key", ModelSecrets.SECRET_SETTINGS));
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
}
