/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.TEST_REQUEST_TIMEOUT;
import static org.elasticsearch.test.ESTestCase.safeGet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class IntegrationTestUtils {
    private IntegrationTestUtils() {}

    public static void createInferenceEndpoint(Client client, TaskType taskType, String inferenceId, Map<String, Object> serviceSettings)
        throws IOException {
        final String service = switch (taskType) {
            case TEXT_EMBEDDING -> TestDenseInferenceServiceExtension.TestInferenceService.NAME;
            case SPARSE_EMBEDDING -> TestSparseInferenceServiceExtension.TestInferenceService.NAME;
            default -> throw new IllegalArgumentException("Unhandled task type [" + taskType + "]");
        };

        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("service", service);
            builder.field("service_settings", serviceSettings);
            builder.endObject();

            content = BytesReference.bytes(builder);
        }

        PutInferenceModelAction.Request request = new PutInferenceModelAction.Request(
            taskType,
            inferenceId,
            content,
            XContentType.JSON,
            TEST_REQUEST_TIMEOUT
        );
        var responseFuture = client.execute(PutInferenceModelAction.INSTANCE, request);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getModel().getInferenceEntityId(), equalTo(inferenceId));
    }

    public static void deleteInferenceEndpoint(Client client, TaskType taskType, String inferenceId) {
        assertAcked(
            safeGet(
                client.execute(
                    DeleteInferenceEndpointAction.INSTANCE,
                    new DeleteInferenceEndpointAction.Request(inferenceId, taskType, true, false)
                )
            )
        );
    }

    public static void deleteIndex(Client client, String indexName) {
        assertAcked(
            safeGet(
                client.admin()
                    .indices()
                    .prepareDelete(indexName)
                    .setIndicesOptions(
                        IndicesOptions.builder().concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(true)).build()
                    )
                    .execute()
            )
        );
    }

    public static XContentBuilder generateSemanticTextMapping(Map<String, String> semanticTextFields) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        for (var entry : semanticTextFields.entrySet()) {
            mapping.startObject(entry.getKey());
            mapping.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
            mapping.field("inference_id", entry.getValue());
            mapping.endObject();
        }
        mapping.endObject().endObject();

        return mapping;
    }
}
