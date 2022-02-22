/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

public class TransformDeprecations {

    public static final String UPGRADE_TRANSFORM_URL = "https://ela.st/es-7-upgrade-transforms";

    public static final String QUERY_BREAKING_CHANGES_URL = "https://ela.st/es-deprecation-8-transform-query-options";

    public static final String AGGS_BREAKING_CHANGES_URL = "https://ela.st/es-deprecation-8-transform-aggregation-options";

    public static final String PAINLESS_BREAKING_CHANGES_URL = "https://ela.st/es-deprecation-8-transform-painless-options";

    public static final String ACTION_UPGRADE_TRANSFORMS_API =
        "This transform configuration uses obsolete syntax which will be unsupported after the next upgrade. "
            + "Use [/_transform/_upgrade] to upgrade all transforms to the latest format.";

    public static final String ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED =
        "Remove [max_page_search_size] from the pivot and configure [max_page_search_size] in the transform settings. "
            + "Alternatively use [/_transform/_upgrade] to upgrade all transforms to the latest format.";

    public static final String MAX_PAGE_SEARCH_SIZE_BREAKING_CHANGES_URL = "https://ela.st/es-deprecation-7-transform-max-page-search-size";

    private TransformDeprecations() {}
}
