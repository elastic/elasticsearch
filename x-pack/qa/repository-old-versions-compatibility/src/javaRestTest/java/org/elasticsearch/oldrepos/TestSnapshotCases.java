/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

/**
* Snapshot zip file names for archive and searchable snapshot tests
*/
public final class TestSnapshotCases {
    // Index created in vES_5 - Basic mapping
    public static final String ES_VERSION_5 = "5";

    // Index created in vES_5 - Custom-Analyzer - standard token filter
    public static final String ES_VERSION_5_STANDARD_TOKEN_FILTER = "5_custom_analyzer";

    // Index created in vES_5 - Basic mapping
    public static final String ES_VERSION_6 = "5";

    // Index created in vES_5 - Custom-Analyzer - standard token filter
    public static final String ES_VERSION_6_STANDARD_TOKEN_FILTER = "5_custom_analyzer";

}
