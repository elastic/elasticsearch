/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

public class TransformDeprecations {

    public static final String UPGRADE_TRANSFORM_URL = "https://ela.st/es-7-upgrade-transforms";

    // breaking changes base url for the _next_ major release
    public static final String BREAKING_CHANGES_BASE_URL =
        "https://www.elastic.co/guide/en/elasticsearch/reference/master/migrating-8.0.html";

    public static final String QUERY_BREAKING_CHANGES_URL = "https://ela.st/es-deprecation-8-transform-query-options";

    public static final String AGGS_BREAKING_CHANGES_URL = "https://ela.st/es-deprecation-8-transform-aggregation-options";

    public static final String ACTION_UPGRADE_TRANSFORMS_API = "Use the upgrade transforms API to fix your transforms.";

    public static final String ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED =
        "[max_page_search_size] is deprecated inside pivot. Use settings instead.";

    public static final String MAX_PAGE_SEARCH_SIZE_BREAKING_CHANGES_URL = "https://ela.st/es-deprecation-7-transform-max-page-search-size";
    private TransformDeprecations() {}
}
