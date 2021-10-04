/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

public class TransformDeprecations {
    public static final String BREAKING_CHANGES_BASE_URL =
        "https://www.elastic.co/guide/en/elasticsearch/reference/master/migrating-8.0.html";

    public static final String QUERY_BREAKING_CHANGES_URL = BREAKING_CHANGES_BASE_URL + "#breaking_80_search_changes";

    public static final String AGGS_BREAKING_CHANGES_URL = BREAKING_CHANGES_BASE_URL + "#breaking_80_aggregations_changes";

    private TransformDeprecations() {}
}
