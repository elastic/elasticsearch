/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

public final class DataFrameAnalyticsFields {

    public static final String ID = "_id_copy";

    // Metadata fields
    static final String CREATION_DATE_MILLIS = "creation_date_in_millis";
    static final String VERSION = "version";
    static final String CREATED = "created";
    static final String CREATED_BY = "created_by";
    static final String ANALYTICS = "analytics";

    private DataFrameAnalyticsFields() {}
}
