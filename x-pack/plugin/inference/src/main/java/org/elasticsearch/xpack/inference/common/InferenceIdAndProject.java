/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.cluster.metadata.ProjectId;

/**
 * A composite cache key pairing an inference entity ID with the project it belongs to.
 * Shared across caches (e.g. endpoint registry, OAuth2 token cache) that need to scope
 * entries per-inference-endpoint and per-project.
 */
public record InferenceIdAndProject(String inferenceEntityId, ProjectId projectId) {}
