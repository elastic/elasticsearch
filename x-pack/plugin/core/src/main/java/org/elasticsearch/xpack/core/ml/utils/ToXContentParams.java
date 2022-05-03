/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.utils;

/**
 * Parameters used by machine learning for controlling X Content serialisation.
 */
public final class ToXContentParams {

    /**
     * Parameter to indicate whether we are serialising to X Content for
     * internal storage. Certain fields need to be persisted but should
     * not be visible everywhere.
     */
    public static final String FOR_INTERNAL_STORAGE = "for_internal_storage";

    /**
     * Parameter to indicate if this XContent serialization should only include fields that are allowed to be used
     * on PUT
     *
     * This helps to GET a configuration, copy it, and then PUT it directly without removing or changing any fields in between
     */
    public static final String EXCLUDE_GENERATED = "exclude_generated";

    /**
     * When serialising POJOs to X Content this indicates whether the calculated (i.e. not stored) fields
     * should be included or not
     */
    public static final String INCLUDE_CALCULATED_FIELDS = "include_calculated_fields";

    private ToXContentParams() {}
}
