/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

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
    Map<String, Object> secrets
) {}
