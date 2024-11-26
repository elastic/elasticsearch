/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionRequest;

import java.util.List;

public record UnifiedRequest(
    List<UnifiedCompletionRequest.Message> messages,
    @Nullable String model,
    @Nullable Long maxCompletionTokens,
    @Nullable Integer n,
    @Nullable UnifiedCompletionRequest.Stop stop,
    @Nullable Float temperature,
    @Nullable UnifiedCompletionRequest.ToolChoice toolChoice,
    @Nullable List<UnifiedCompletionRequest.Tool> tool,
    @Nullable Float topP,
    @Nullable String user,
    boolean stream
) {}
