/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.openai.OpenAiTextEmbeddingPayload;

import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SageMakerSchemasTests extends ESTestCase {
    public static SageMakerSchemas mockSchemas() {
        SageMakerSchemas schemas = mock();
        var schema = mockSchema();
        when(schemas.schemaFor(any(TaskType.class), anyString())).thenReturn(schema);
        return schemas;
    }

    public static SageMakerSchema mockSchema() {
        SageMakerSchema schema = mock();
        when(schema.apiServiceSettings(anyMap(), any())).thenReturn(SageMakerStoredServiceSchema.NO_OP);
        when(schema.apiTaskSettings(anyMap(), any())).thenReturn(SageMakerStoredTaskSchema.NO_OP);
        return schema;
    }

    private static final SageMakerSchemas schemas = new SageMakerSchemas();

    public void testSupportedTaskTypes() {
        assertThat(schemas.supportedTaskTypes(), containsInAnyOrder(TaskType.TEXT_EMBEDDING));
    }

    public void testSupportedStreamingTasks() {
        assertThat(schemas.supportedStreamingTasks(), empty());
    }

    public void testSchemaFor() {
        var payloads = Stream.of(new OpenAiTextEmbeddingPayload());
        payloads.forEach(payload -> {
            payload.supportedTasks().forEach(taskType -> {
                var model = mockModel(taskType, payload.api());
                assertNotNull(schemas.schemaFor(model));
            });
        });
    }

    public void testStreamSchemaFor() {
        var payloads = Stream.<SageMakerStreamSchemaPayload>of(/* For when we add support for streaming payloads */);
        payloads.forEach(payload -> {
            payload.supportedTasks().forEach(taskType -> {
                var model = mockModel(taskType, payload.api());
                assertNotNull(schemas.streamSchemaFor(model));
            });
        });
    }

    private SageMakerModel mockModel(TaskType taskType, String api) {
        SageMakerModel model = mock();
        when(model.getTaskType()).thenReturn(taskType);
        when(model.api()).thenReturn(api);
        return model;
    }

    public void testMissingTaskTypeThrowsException() {
        var knownPayload = new OpenAiTextEmbeddingPayload();
        var unknownTaskType = TaskType.COMPLETION;
        var knownModel = mockModel(unknownTaskType, knownPayload.api());
        assertThrows(
            "Task [completion] is not compatible for service [sagemaker] and api [openai]. Supported tasks: [text_embedding]",
            ElasticsearchStatusException.class,
            () -> schemas.schemaFor(knownModel)
        );
    }

    public void testMissingSchemaThrowsException() {
        var unknownModel = mockModel(TaskType.ANY, "blah");
        assertThrows(
            "Task [any] is not compatible for service [sagemaker] and api [blah]. Supported tasks: []",
            ElasticsearchStatusException.class,
            () -> schemas.schemaFor(unknownModel)
        );
    }

    public void testMissingStreamSchemaThrowsException() {
        var unknownModel = mockModel(TaskType.ANY, "blah");
        assertThrows(
            "Streaming is not allowed for service [sagemaker], api [blah], and task [any]. Supported streaming tasks: []",
            ElasticsearchStatusException.class,
            () -> schemas.streamSchemaFor(unknownModel)
        );
    }

    public void testNamedWriteables() {
        var namedWriteables = Stream.of(new OpenAiTextEmbeddingPayload().namedWriteables());

        var expectedNamedWriteables = Stream.concat(
            namedWriteables.flatMap(names -> names.map(entry -> entry.name)),
            Stream.of(SageMakerStoredServiceSchema.NO_OP.getWriteableName(), SageMakerStoredTaskSchema.NO_OP.getWriteableName())
        ).distinct().toArray();

        var actualRegisteredNames = SageMakerSchemas.namedWriteables().stream().map(entry -> entry.name).toList();

        assertThat(actualRegisteredNames, containsInAnyOrder(expectedNamedWriteables));
    }
}
