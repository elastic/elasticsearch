/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.inference.metadata.EndpointMetadata;

import java.util.HashMap;
import java.util.Map;

/**
 * Semi parsed model where inference entity id, task type and service
 * are known but the settings are not parsed.
 */
public record UnparsedModel(
    String inferenceEntityId,
    TaskType taskType,
    String service,
    Map<String, Object> settings,
    Map<String, Object> secrets,
    EndpointMetadata endpointMetadata
) {
    public UnparsedModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> settings,
        Map<String, Object> secrets
    ) {
        this(inferenceEntityId, taskType, service, settings, secrets, EndpointMetadata.EMPTY_INSTANCE);
    }

    public UnparsedModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> settings,
        Map<String, Object> secrets,
        EndpointMetadata endpointMetadata
    ) {
        this.inferenceEntityId = inferenceEntityId;
        this.taskType = taskType;
        this.service = service;

        // We ensure that settings and secrets maps are modifiable because during parsing we are removing from them
        this.settings = settings == null ? null : new HashMap<>(settings);
        // Additionally, an empty secrets map is treated as null in order to skip potential validations for missing keys
        // which should not be necessary when parsing a persisted model.
        this.secrets = secrets == null || secrets.isEmpty() ? null : new HashMap<>(secrets);

        this.endpointMetadata = endpointMetadata;
    }
}
