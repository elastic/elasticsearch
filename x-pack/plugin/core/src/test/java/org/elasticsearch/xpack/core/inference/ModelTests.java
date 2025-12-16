/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
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
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ModelTests extends AbstractBWCWireSerializationTestCase<Model> {
    public static Model randomModel() {
        return new Model(
            new ModelConfigurations(
                randomAlphaOfLength(6),
                randomFrom(TaskType.values()),
                randomAlphaOfLength(6),
                new TestServiceSettings(
                    randomAlphaOfLength(10),
                    randomIntBetween(1, 1024),
                    randomFrom(SimilarityMeasure.values()),
                    randomFrom(DenseVectorFieldMapper.ElementType.values())
                ),
                EmptyTaskSettings.INSTANCE,
                randomBoolean() ? ChunkingSettingsTests.createRandomChunkingSettings() : null
            ),
            new ModelSecrets(EmptySecretSettings.INSTANCE)
        );
    }

    public record TestServiceSettings(
        String model,
        Integer dimensions,
        @Nullable SimilarityMeasure similarity,
        @Nullable DenseVectorFieldMapper.ElementType elementType
    ) implements ServiceSettings {

        static final String NAME = "test_text_embedding_service_settings";

        public TestServiceSettings(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readInt(),
                in.readOptionalEnum(SimilarityMeasure.class),
                in.readOptionalEnum(DenseVectorFieldMapper.ElementType.class)
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("model", model);
            builder.field("dimensions", dimensions);
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
            out.writeInt(dimensions);
            out.writeOptionalEnum(similarity);
            out.writeOptionalEnum(elementType);
        }

        @Override
        public ToXContentObject getFilteredXContentObject() {
            return this;
        }

        @Override
        public SimilarityMeasure similarity() {
            return similarity != null ? similarity : SimilarityMeasure.COSINE;
        }

        @Override
        public DenseVectorFieldMapper.ElementType elementType() {
            return elementType != null ? elementType : DenseVectorFieldMapper.ElementType.FLOAT;
        }

        @Override
        public String modelId() {
            return model;
        }
    }

    public record SimpleSecretSettings(String field) implements SecretSettings {
        public static final String NAME = "simple_secret_settings";
        private static final String FIELD_KEY = "field";

        public SimpleSecretSettings {
            Objects.requireNonNull(field);
        }

        public SimpleSecretSettings(StreamInput in) throws IOException {
            this(in.readString());
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
            out.writeString(field);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FIELD_KEY, field);
            builder.endObject();
            return builder;
        }

        @Override
        public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
            if (newSecrets == null) {
                return null;
            }

            ValidationException validationException = new ValidationException();
            var value = newSecrets.get(FIELD_KEY);
            if (value == null) {
                validationException.addValidationError("Missing required secret setting: " + FIELD_KEY);
                throw validationException;
            } else if (value instanceof String == false) {
                validationException.addValidationError("Expected secret setting [" + FIELD_KEY + "] to be of type String");
                throw validationException;
            }
            return new SimpleSecretSettings((String) value);
        }
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, TestServiceSettings.NAME, TestServiceSettings::new),
            new NamedWriteableRegistry.Entry(SecretSettings.class, SimpleSecretSettings.NAME, SimpleSecretSettings::new)
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        var namedWriteables = new ArrayList<NamedWriteableRegistry.Entry>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(TaskSettings.class, EmptyTaskSettings.NAME, EmptyTaskSettings::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SecretSettings.class, EmptySecretSettings.NAME, EmptySecretSettings::new));
        namedWriteables.addAll(getNamedWriteables());
        namedWriteables.addAll(XPackClientPlugin.getChunkingSettingsNamedWriteables());

        return new NamedWriteableRegistry(namedWriteables);
    }

    @Override
    protected Model mutateInstanceForVersion(Model instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<Model> instanceReader() {
        return Model::new;
    }

    @Override
    protected Model createTestInstance() {
        return randomModel();
    }

    @Override
    protected Model mutateInstance(Model instance) throws IOException {
        int choice = randomIntBetween(0, 1);
        switch (choice) {
            case 0 -> {
                var originalConfig = instance.getConfigurations();
                ModelConfigurations mutatedConfig = new ModelConfigurations(
                    originalConfig.getInferenceEntityId() + "_mutated",
                    originalConfig.getTaskType(),
                    originalConfig.getService(),
                    originalConfig.getServiceSettings(),
                    originalConfig.getTaskSettings(),
                    originalConfig.getChunkingSettings()
                );
                return new Model(mutatedConfig, instance.getSecrets());
            }
            case 1 -> {
                return new Model(instance.getConfigurations(), new ModelSecrets(new SimpleSecretSettings(randomAlphaOfLength(10))));
            }
            default -> throw new IllegalStateException("Unexpected value: " + choice);
        }
    }
}
