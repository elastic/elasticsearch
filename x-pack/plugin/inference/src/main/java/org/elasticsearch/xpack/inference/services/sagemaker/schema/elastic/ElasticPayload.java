/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import software.amazon.awssdk.core.SdkBytes;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemaPayload;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;

import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;
import static org.elasticsearch.xpack.core.inference.action.InferenceAction.Request.INPUT;
import static org.elasticsearch.xpack.core.inference.action.InferenceAction.Request.INPUT_TYPE;
import static org.elasticsearch.xpack.core.inference.action.InferenceAction.Request.QUERY;

interface ElasticPayload extends SageMakerSchemaPayload {
    String API = "elastic";
    String APPLICATION_JSON = jsonXContent.type().mediaTypeWithoutParameters();
    /**
     * If Elastic receives an element in the response that it does not recognize, it will fail.
     */
    boolean IGNORE_UNKNOWN_FIELDS = false;

    @Override
    default String api() {
        return API;
    }

    @Override
    default String accept(SageMakerModel model) {
        return APPLICATION_JSON;
    }

    @Override
    default String contentType(SageMakerModel model) {
        return APPLICATION_JSON;
    }

    @Override
    default SdkBytes requestBytes(SageMakerModel model, SageMakerInferenceRequest request) throws Exception {
        if (model.apiTaskSettings() instanceof SageMakerElasticTaskSettings elasticTaskSettings) {
            return SdkBytes.fromUtf8String(Strings.toString((builder, params) -> {
                if (request.input().size() > 1) {
                    builder.field(INPUT.getPreferredName(), request.input());
                } else {
                    builder.field(INPUT.getPreferredName(), request.input().get(0));
                }
                if (InputType.isSpecified(request.inputType())) {
                    builder.field(INPUT_TYPE.getPreferredName(), switch (request.inputType()) {
                        case INGEST, INTERNAL_INGEST -> InputType.INGEST;
                        case SEARCH, INTERNAL_SEARCH -> InputType.SEARCH;
                        default -> request.inputType();
                    });
                }
                if (request.query() != null) {
                    builder.field(QUERY.getPreferredName(), request.query());
                }
                if (elasticTaskSettings.isEmpty() == false) {
                    builder.field(InferenceAction.Request.TASK_SETTINGS.getPreferredName());
                    if (elasticTaskSettings.isFragment()) {
                        builder.startObject();
                    }
                    builder.value(elasticTaskSettings);
                    if (elasticTaskSettings.isFragment()) {
                        builder.endObject();
                    }
                }
                return builder;
            }));
        } else {
            throw createUnsupportedSchemaException(model);
        }
    }

    @Override
    default SageMakerElasticTaskSettings apiTaskSettings(Map<String, Object> taskSettings, ValidationException validationException) {
        if (taskSettings != null && (taskSettings.isEmpty() == false)) {
            validationException.addValidationError(
                InferenceAction.Request.TASK_SETTINGS.getPreferredName()
                    + " is only supported during the inference request and cannot be stored in the inference endpoint."
            );
        }
        return SageMakerElasticTaskSettings.empty();
    }

    @Override
    default Stream<NamedWriteableRegistry.Entry> namedWriteables() {
        return Stream.of(
            new NamedWriteableRegistry.Entry(
                SageMakerStoredTaskSchema.class,
                SageMakerElasticTaskSettings.NAME,
                SageMakerElasticTaskSettings::new
            )
        );
    }
}
