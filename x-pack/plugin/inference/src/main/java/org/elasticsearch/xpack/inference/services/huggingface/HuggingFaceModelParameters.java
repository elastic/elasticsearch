/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.util.Map;

public record HuggingFaceModelParameters(
    String inferenceEntityId,
    TaskType taskType,
    Map<String, Object> serviceSettings,
    Map<String, Object> taskSettings,
    ChunkingSettings chunkingSettings,
    Map<String, Object> secretSettings,
    String failureMessage,
    ConfigurationParseContext context
) {}
