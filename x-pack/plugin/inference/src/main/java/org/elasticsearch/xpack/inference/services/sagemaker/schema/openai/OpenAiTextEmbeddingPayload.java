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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
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

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

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
    public SageMakerStoredServiceSchema apiServiceSettings(
        Map<String, Object> serviceSettings,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        return ApiServiceSettings.fromMap(serviceSettings, context, validationException);
    }

    @Override
    public SageMakerStoredTaskSchema apiTaskSettings(Map<String, Object> taskSettings, ValidationException validationException) {
        return SageMakerOpenAiTaskSettings.fromMap(taskSettings, validationException);
    }

    @Override
    public Stream<NamedWriteableRegistry.Entry> namedWriteables() {
        return Stream.of(
            new NamedWriteableRegistry.Entry(SageMakerStoredServiceSchema.class, ApiServiceSettings.NAME, ApiServiceSettings::new),
            new NamedWriteableRegistry.Entry(
                SageMakerStoredTaskSchema.class,
                SageMakerOpenAiTaskSettings.NAME,
                SageMakerOpenAiTaskSettings::new
            )
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
        if (model.apiServiceSettings() instanceof ApiServiceSettings apiServiceSettings
            && model.apiTaskSettings() instanceof SageMakerOpenAiTaskSettings apiTaskSettings) {
            try (var builder = JsonXContent.contentBuilder()) {
                builder.startObject();
                if (request.query() != null) {
                    builder.field("query", request.query());
                }
                if (request.input().size() == 1) {
                    builder.field("input", request.input().get(0));
                } else {
                    builder.field("input", request.input());
                }
                if (apiTaskSettings.user() != null) {
                    builder.field("user", apiTaskSettings.user());
                }
                if (apiServiceSettings.dimensionsSetByUser() && apiServiceSettings.dimensions() != null) {
                    builder.field("dimensions", apiServiceSettings.dimensions());
                }
                builder.endObject();
                return SdkBytes.fromUtf8String(Strings.toString(builder));
            }
        } else {
            throw createUnsupportedSchemaException(model);
        }
    }

    @Override
    public DenseEmbeddingFloatResults responseBody(SageMakerModel model, InvokeEndpointResponse response) throws Exception {
        try (var p = jsonXContent.createParser(XContentParserConfiguration.EMPTY, response.body().asInputStream())) {
            return OpenAiEmbeddingsResponseEntity.parse(p).toDenseEmbeddingFloatResults();
        }
    }

    record ApiServiceSettings(@Nullable Integer dimensions, Boolean dimensionsSetByUser) implements SageMakerStoredServiceSchema {
        private static final String NAME = "sagemaker_openai_text_embeddings_service_settings";
        private static final String DIMENSIONS_FIELD = "dimensions";
        private static final TransportVersion ML_INFERENCE_SAGEMAKER = TransportVersion.fromName("ml_inference_sagemaker");

        ApiServiceSettings(StreamInput in) throws IOException {
            this(in.readOptionalInt(), in.readBoolean());
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            assert false : "should never be called when supportsVersion is used";
            return ML_INFERENCE_SAGEMAKER;
        }

        @Override
        public boolean supportsVersion(TransportVersion version) {
            return version.supports(ML_INFERENCE_SAGEMAKER);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalInt(dimensions);
            out.writeBoolean(dimensionsSetByUser);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (dimensions != null) {
                builder.field(DIMENSIONS_FIELD, dimensions);
            }
            builder.field(ServiceFields.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
            return builder;
        }

        @Override
        public ToXContentObject getFilteredXContentObject() {
            // dimensions_set_by_user is internal: it is persisted by toXContent but must not be returned in the GET response.
            return new ToXContentObject() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    if (dimensions != null) {
                        builder.field(DIMENSIONS_FIELD, dimensions);
                    }
                    return builder;
                }

                @Override
                public boolean isFragment() {
                    return true;
                }
            };
        }

        static ApiServiceSettings fromMap(
            Map<String, Object> serviceSettings,
            ConfigurationParseContext context,
            ValidationException validationException
        ) {
            var dimensions = extractOptionalPositiveInteger(
                serviceSettings,
                DIMENSIONS_FIELD,
                ModelConfigurations.SERVICE_SETTINGS,
                validationException
            );
            // dimensions_set_by_user is internal and not user-settable. In a request we intentionally do not read it, so that a
            // user-supplied value is rejected as an unknown setting; the flag is derived from whether dimensions were provided.
            // In a persisted config we read the stored value, defaulting to false for configs persisted before the field existed.
            boolean dimensionsSetByUser;
            if (ConfigurationParseContext.isRequestContext(context)) {
                dimensionsSetByUser = dimensions != null;
            } else {
                var storedDimensionsSetByUser = extractOptionalBoolean(
                    serviceSettings,
                    ServiceFields.DIMENSIONS_SET_BY_USER,
                    validationException
                );
                dimensionsSetByUser = storedDimensionsSetByUser != null && storedDimensionsSetByUser;
            }

            return new ApiServiceSettings(dimensions, dimensionsSetByUser);
        }

        @Override
        public SimilarityMeasure similarity() {
            return SimilarityMeasure.DOT_PRODUCT;
        }

        @Override
        public DenseVectorFieldMapper.ElementType elementType() {
            return DenseVectorFieldMapper.ElementType.FLOAT;
        }

        @Override
        public SageMakerStoredServiceSchema updateModelWithEmbeddingDetails(Integer dimensions) {
            return new ApiServiceSettings(dimensions, false);
        }
    }
}
