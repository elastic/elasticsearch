/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceRequestMetadata;

/**
 * Snapshot of the headers sent on an Elastic Inference Service request.
 * @param attribution request attribution, including product origin, captured before the outbound call is executed
 * @param esVersion the Elasticsearch version of the node handling the request
 */
public record ElasticInferenceServiceRequestMetadata(InferenceRequestMetadata attribution, @Nullable String esVersion) {}
