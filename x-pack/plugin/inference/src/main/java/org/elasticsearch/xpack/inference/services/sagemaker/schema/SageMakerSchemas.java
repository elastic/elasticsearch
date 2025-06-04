/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.openai.OpenAiCompletionPayload;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.openai.OpenAiTextEmbeddingPayload;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;

/**
 * The mapping and registry for all supported model API.
 */
public class SageMakerSchemas {
    private static final Map<TaskAndApi, SageMakerSchema> schemas;
    private static final Map<TaskAndApi, SageMakerStreamSchema> streamSchemas;
    private static final Map<String, Set<TaskType>> tasksByApi;
    private static final Map<String, Set<TaskType>> streamingTasksByApi;
    private static final Set<TaskType> supportedStreamingTasks;
    private static final EnumSet<TaskType> supportedTaskTypes;

    static {
        /*
         * Add new model API to the register call.
         */
        schemas = register(new OpenAiTextEmbeddingPayload(), new OpenAiCompletionPayload());

        streamSchemas = schemas.entrySet()
            .stream()
            .filter(e -> e.getValue() instanceof SageMakerStreamSchema)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (SageMakerStreamSchema) e.getValue()));

        tasksByApi = schemas.keySet()
            .stream()
            .collect(Collectors.groupingBy(TaskAndApi::api, Collectors.mapping(TaskAndApi::taskType, Collectors.toSet())));
        streamingTasksByApi = streamSchemas.keySet()
            .stream()
            .collect(Collectors.groupingBy(TaskAndApi::api, Collectors.mapping(TaskAndApi::taskType, Collectors.toSet())));

        supportedStreamingTasks = streamSchemas.keySet().stream().map(TaskAndApi::taskType).collect(Collectors.toSet());
        supportedTaskTypes = EnumSet.copyOf(schemas.keySet().stream().map(TaskAndApi::taskType).collect(Collectors.toSet()));
    }

    private static Map<TaskAndApi, SageMakerSchema> register(SageMakerSchemaPayload... payloads) {
        return Arrays.stream(payloads).flatMap(payload -> payload.supportedTasks().stream().map(taskType -> {
            var key = new TaskAndApi(taskType, payload.api());
            SageMakerSchema value;
            if (payload instanceof SageMakerStreamSchemaPayload streamPayload) {
                value = new SageMakerStreamSchema(streamPayload);
            } else {
                value = new SageMakerSchema(payload);
            }
            return Map.entry(key, value);
        })).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Automatically register the stored Schema writeables with {@link org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider}.
     */
    public static List<NamedWriteableRegistry.Entry> namedWriteables() {
        return Stream.concat(
            Stream.of(
                new NamedWriteableRegistry.Entry(
                    SageMakerStoredServiceSchema.class,
                    SageMakerStoredServiceSchema.NO_OP.getWriteableName(),
                    in -> SageMakerStoredServiceSchema.NO_OP
                ),
                new NamedWriteableRegistry.Entry(
                    SageMakerStoredTaskSchema.class,
                    SageMakerStoredTaskSchema.NO_OP.getWriteableName(),
                    in -> SageMakerStoredTaskSchema.NO_OP
                )
            ),
            schemas.values().stream().flatMap(SageMakerSchema::namedWriteables)
        )
            // Dedupe based on Entry name, we allow Payloads to declare the same Entry but the Registry does not handle duplicates
            .collect(
                () -> new HashMap<String, NamedWriteableRegistry.Entry>(),
                (map, entry) -> map.putIfAbsent(entry.name, entry),
                Map::putAll
            )
            .values()
            .stream()
            .toList();
    }

    public SageMakerSchema schemaFor(SageMakerModel model) throws ElasticsearchStatusException {
        return schemaFor(model.getTaskType(), model.api());
    }

    public SageMakerSchema schemaFor(TaskType taskType, String api) throws ElasticsearchStatusException {
        var schema = schemas.get(new TaskAndApi(taskType, api));
        if (schema == null) {
            throw new ElasticsearchStatusException(
                format(
                    "Task [%s] is not compatible for service [sagemaker] and api [%s]. Supported tasks: [%s]",
                    api,
                    taskType.toString(),
                    tasksByApi.getOrDefault(api, Set.of())
                ),
                RestStatus.METHOD_NOT_ALLOWED
            );
        }
        return schema;
    }

    public SageMakerStreamSchema streamSchemaFor(SageMakerModel model) throws ElasticsearchStatusException {
        var schema = streamSchemas.get(new TaskAndApi(model.getTaskType(), model.api()));
        if (schema == null) {
            throw new ElasticsearchStatusException(
                format(
                    "Streaming is not allowed for service [sagemaker], api [%s], and task [%s]. Supported streaming tasks: [%s]",
                    model.api(),
                    model.getTaskType().toString(),
                    streamingTasksByApi.getOrDefault(model.api(), Set.of())
                ),
                RestStatus.METHOD_NOT_ALLOWED
            );
        }
        return schema;
    }

    public EnumSet<TaskType> supportedTaskTypes() {
        return supportedTaskTypes;
    }

    public Set<TaskType> supportedStreamingTasks() {
        return supportedStreamingTasks;
    }
}
