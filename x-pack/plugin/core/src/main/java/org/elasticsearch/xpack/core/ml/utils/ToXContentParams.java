/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
     * When serialising POJOs to X Content this indicates whether the type field
     * should be included or not
     */
    public static final String INCLUDE_TYPE = "include_type";

    /**
     * When serialising POJOs to X Content this indicates whether the calculated (i.e. not stored) fields
     * should be included or not
     */
    public static final String INCLUDE_CALCULATED_FIELDS = "include_calculated_fields";

    private ToXContentParams() {
    }
}
