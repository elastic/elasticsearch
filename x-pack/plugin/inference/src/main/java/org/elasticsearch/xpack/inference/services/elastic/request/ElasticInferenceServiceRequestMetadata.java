/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

/**
 * Record encapsulating arbitrary metadata, which is usually propagated through HTTP headers.
 * @param productOrigin - product origin of the inference request (usually a whole system like "kibana", "logstash" etc.)
 * @param productUseCase - product use case of the inference request (more granular view on a user flow like "security ai assistant" etc.)
 */
public record ElasticInferenceServiceRequestMetadata(String productOrigin, String productUseCase) {}
