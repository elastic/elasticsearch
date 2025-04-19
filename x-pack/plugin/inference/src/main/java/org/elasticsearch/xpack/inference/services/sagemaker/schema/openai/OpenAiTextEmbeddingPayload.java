/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.openai;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemaPayload;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredServiceSchema;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

public class OpenAiTextEmbeddingPayload implements SageMakerSchemaPayload {

    private static final XContent jsonXContent = JsonXContent.jsonXContent;
    private static final String APPLICATION_JSON = jsonXContent.type().mediaTypeWithoutParameters();

    @Override
    public String api() {
        return "openai";
    }

    @Override
    public EnumSet<TaskType> supportedTasks() {
        return EnumSet.of(TaskType.TEXT_EMBEDDING);
    }

    @Override
    public SageMakerStoredServiceSchema extraServiceSettings(Map<String, Object> serviceSettings, ValidationException validationException) {
        return ExtraServiceSettings.fromMap(serviceSettings, validationException);
    }

    @Override
    public SageMakerStoredTaskSchema extraTaskSettings(Map<String, Object> taskSettings, ValidationException validationException) {
        return new ExtraTaskSettings.ExtraTaskSettingsBuilder().fromMap(taskSettings, validationException).build();
    }

    @Override
    public Stream<NamedWriteableRegistry.Entry> namedWriteables() {
        return Stream.of(
            new NamedWriteableRegistry.Entry(SageMakerStoredServiceSchema.class, ExtraServiceSettings.NAME, ExtraServiceSettings::new),
            new NamedWriteableRegistry.Entry(SageMakerStoredTaskSchema.class, ExtraTaskSettings.NAME, ExtraTaskSettings::new)
        );
    }

    @Override
    public String accept(SageMakerModel model) {
        return APPLICATION_JSON;
    }

    @Override
    public String contentType(SageMakerModel model) {
        return APPLICATION_JSON;
    }

    @Override
    public SdkBytes requestBytes(SageMakerModel model, SageMakerInferenceRequest request) throws Exception {
        if (model.extraServiceSettings() instanceof ExtraServiceSettings extraServiceSettings
            && model.extraTaskSettings() instanceof ExtraTaskSettings extraTaskSettings) {
            try (var builder = JsonXContent.contentBuilder()) {
                builder.startObject();
                if (request.query() != null) {
                    builder.field("query", request.query());
                }
                if (request.input() != null) {
                    if (request.input().size() == 1) {
                        builder.field("input", request.input().get(0));
                    } else {
                        builder.array("input", request.input());
                    }
                }
                if (extraTaskSettings.user() != null) {
                    builder.field("user", extraTaskSettings.user());
                }
                if (extraServiceSettings.dimensions() != null) {
                    builder.field("dimensions", extraServiceSettings.dimensions());
                }
                // organizationId is supposed to be a header, but SageMaker SDK does not allow headers, so add it to the body
                if (extraServiceSettings.organizationId() != null) {
                    builder.field("organization_id", extraServiceSettings.organizationId());
                }
                builder.endObject();
                return SdkBytes.fromUtf8String(Strings.toString(builder));
            }
        } else {
            throw createUnsupportedSchemaException(model);
        }
    }

    @Override
    public InferenceServiceResults responseBody(SageMakerModel model, InvokeEndpointResponse response) throws Exception {
        try (var p = jsonXContent.createParser(XContentParserConfiguration.EMPTY, response.body().asInputStream())) {
            return OpenAiEmbeddingsResponseEntity.EmbeddingFloatResult.PARSER.apply(p, null).toTextEmbeddingFloatResults();
        }
    }

    record ExtraServiceSettings(@Nullable Integer dimensions, @Nullable String organizationId) implements SageMakerStoredServiceSchema {
        private static final String NAME = "sagemaker_openai_text_embeddings_service_settings";
        private static final String DIMENSIONS_FIELD = "dimensions";
        private static final String ORGANIZATION_FIELD = "organization_id";

        ExtraServiceSettings(StreamInput in) throws IOException {
            this(in.readOptionalInt(), in.readOptionalString());
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalInt(dimensions);
            out.writeOptionalString(organizationId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (dimensions != null) {
                builder.field(DIMENSIONS_FIELD, dimensions);
            }
            if (organizationId != null) {
                builder.field(ORGANIZATION_FIELD, organizationId);
            }
            return builder;
        }

        static ExtraServiceSettings fromMap(Map<String, Object> serviceSettings, ValidationException validationException) {
            var dimensions = extractOptionalPositiveInteger(
                serviceSettings,
                DIMENSIONS_FIELD,
                ModelConfigurations.SERVICE_SETTINGS,
                validationException
            );
            var organizationId = extractOptionalString(
                serviceSettings,
                ORGANIZATION_FIELD,
                ModelConfigurations.SERVICE_SETTINGS,
                validationException
            );

            return new ExtraServiceSettings(dimensions, organizationId);
        }
    }

    record ExtraTaskSettings(@Nullable String user) implements SageMakerStoredTaskSchema {
        private static final String NAME = "sagemaker_openai_text_embeddings_task_settings";
        private static final String USER_FIELD = "user";

        ExtraTaskSettings(StreamInput in) throws IOException {
            this(in.readOptionalString());
        }

        @Override
        public Builder toBuilder() {
            return new ExtraTaskSettingsBuilder();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(user);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return user != null ? builder.field(USER_FIELD, user) : builder;
        }

        static class ExtraTaskSettingsBuilder implements Builder {
            private String user;

            @Override
            public ExtraTaskSettingsBuilder fromMap(Map<String, Object> map, ValidationException exception) {
                user = extractOptionalString(map, USER_FIELD, ModelConfigurations.SERVICE_SETTINGS, exception);
                return this;
            }

            @Override
            public ExtraTaskSettings build() {
                return new ExtraTaskSettings(user);
            }
        }
    }
}
