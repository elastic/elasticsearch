/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.test.rest;

import java.util.List;

public final class XPackRestTestConstants {

    // Watcher constants:
    public static final String INDEX_TEMPLATE_VERSION = "10";
    public static final String HISTORY_TEMPLATE_NAME_NO_ILM = ".watch-history-no-ilm-" + INDEX_TEMPLATE_VERSION;

    public static final String[] TEMPLATE_NAMES_NO_ILM = new String[] { HISTORY_TEMPLATE_NAME_NO_ILM };

    // ML constants:
    public static final String RESULTS_INDEX_PREFIX = ".ml-anomalies-";
    public static final String STATE_INDEX_PREFIX = ".ml-state";

    public static final List<String> ML_POST_V7120_TEMPLATES = List.of(STATE_INDEX_PREFIX, RESULTS_INDEX_PREFIX);

    // Transform constants:
    public static final String TRANSFORM_TASK_NAME = "data_frame/transforms";
    public static final String TRANSFORM_INTERNAL_INDEX_PREFIX = ".transform-internal-";
    public static final String TRANSFORM_INTERNAL_INDEX_PREFIX_DEPRECATED = ".data-frame-internal-";

    private XPackRestTestConstants() {}
}
