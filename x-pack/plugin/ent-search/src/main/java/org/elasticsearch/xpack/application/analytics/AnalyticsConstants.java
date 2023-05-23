/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

public class AnalyticsConstants {

    private AnalyticsConstants() {}

    // Data stream related constants.
    public static final String EVENT_DATA_STREAM_DATASET = "events";
    public static final String EVENT_DATA_STREAM_TYPE = "behavioral_analytics";
    public static final String EVENT_DATA_STREAM_INDEX_PREFIX = EVENT_DATA_STREAM_TYPE + "-" + EVENT_DATA_STREAM_DATASET + "-";
    public static final String EVENT_DATA_STREAM_INDEX_PATTERN = EVENT_DATA_STREAM_INDEX_PREFIX + "*";

    // Resource config.
    public static final String ROOT_RESOURCE_PATH = "/org/elasticsearch/xpack/entsearch/analytics/";

    // The variable to be replaced with the template version number
    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.entsearch.analytics.template.version";
}
