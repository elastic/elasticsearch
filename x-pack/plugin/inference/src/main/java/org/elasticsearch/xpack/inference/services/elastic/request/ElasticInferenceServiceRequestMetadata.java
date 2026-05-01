/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.inference.telemetry.InferenceProductContext;

/**
 * Record encapsulating arbitrary metadata, which is usually propagated through HTTP headers.
 * @param context - product attribution context holding product use case and product origin
 * @param esVersion - the Elasticsearch version of the node handling the request
 */
public record ElasticInferenceServiceRequestMetadata(InferenceProductContext context, String esVersion) {

    /** Returns the product origin of the inference request (usually a whole system like "kibana", "logstash" etc.) */
    public String productOrigin() {
        return context.productOrigin();
    }

    /** Returns the product use case of the inference request (more granular view on a user flow like "security ai assistant" etc.) */
    public String productUseCase() {
        return context.productUseCase();
    }
}
