/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests;

import java.io.IOException;
import java.util.List;

public class ModelTests extends ESTestCase {
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

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, TestServiceSettings.NAME, TestServiceSettings::new)
        );
    }
}
