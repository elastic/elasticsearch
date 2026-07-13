/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;

import java.util.EnumSet;

import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;

public class ElasticRerankPayload implements ElasticPayload {

    private static final EnumSet<TaskType> SUPPORTED_TASKS = EnumSet.of(TaskType.RERANK);
    private static final ConstructingObjectParser<RankedDocsResults, Void> PARSER = RankedDocsResults.createParser(IGNORE_UNKNOWN_FIELDS);

    @Override
    public EnumSet<TaskType> supportedTasks() {
        return SUPPORTED_TASKS;
    }

    /**
     * {
     *     "input": "single string or list",
     *     "query": "string",
     *     "return_documents": "boolean",
     *     "top_n": "integer",
     *     "task_settings": {
     *         "additional": "settings"
     *     }
     * }
     */
    @Override
    public SdkBytes requestBytes(SageMakerModel model, SageMakerInferenceRequest request) throws Exception {
        if (model.apiTaskSettings() instanceof SageMakerElasticTaskSettings elasticTaskSettings) {
            return SdkBytes.fromUtf8String(Strings.toString((builder, params) -> {
                if (request.input().size() > 1) {
                    builder.field(RerankRequest.INPUT_FIELD, request.input());
                } else {
                    builder.field(RerankRequest.INPUT_FIELD, request.input().get(0));
                }

                assert request.query() != null : "InferenceAction.Request will validate that rerank requests have a query field";
                builder.field(RerankRequest.QUERY_FIELD, request.query());

                if (request.returnDocuments() != null) {
                    builder.field(RerankRequest.RETURN_DOCUMENTS_FIELD, request.returnDocuments());
                }

                if (request.topN() != null) {
                    builder.field(RerankRequest.TOP_N_FIELD, request.topN());
                }

                if (elasticTaskSettings.isEmpty() == false) {
                    builder.field(ModelConfigurations.TASK_SETTINGS);
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

    /**
     * {
     *   "rerank": [
     *     {
     *         "index": 0,
     *         "relevance_score": 1.0
     *         "text": "hello, world"
     *     }
     *   ]
     * }
     */
    @Override
    public RankedDocsResults responseBody(SageMakerModel model, InvokeEndpointResponse response) throws Exception {
        try (var p = jsonXContent.createParser(XContentParserConfiguration.EMPTY, response.body().asInputStream())) {
            return PARSER.apply(p, null);
        }
    }
}
