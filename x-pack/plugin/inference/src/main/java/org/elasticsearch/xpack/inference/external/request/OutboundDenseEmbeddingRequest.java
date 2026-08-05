/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.elasticsearch.inference.TaskType;

/**
 * Marker interface for requests that may be {@link TaskType#TEXT_EMBEDDING} or {@link TaskType#EMBEDDING}. Implementing classes should
 * implement the {@link OutboundRequest#getTaskType()} method to return the appropriate task type.
 */
public interface OutboundDenseEmbeddingRequest extends OutboundRequest {}
