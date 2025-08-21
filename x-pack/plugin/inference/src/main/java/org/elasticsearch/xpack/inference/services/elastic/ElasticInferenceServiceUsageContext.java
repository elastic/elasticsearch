/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import java.util.Locale;

/**
 * Specifies the usage context for a request to the Elastic Inference Service.
 * This helps to determine the type of resources that are allocated in the Elastic Inference Service for the particular request.
 */
public enum ElasticInferenceServiceUsageContext {

    SEARCH,
    INGEST,
    UNSPECIFIED;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

}
