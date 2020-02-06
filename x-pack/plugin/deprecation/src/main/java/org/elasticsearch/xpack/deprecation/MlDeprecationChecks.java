/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.List;

/**
 * Check the {@link DatafeedConfig} query and aggregations for deprecated usages.
 */
final class MlDeprecationChecks {

    private MlDeprecationChecks() {
    }

    static DeprecationIssue checkDataFeedQuery(DatafeedConfig datafeedConfig, NamedXContentRegistry xContentRegistry) {
        List<String> deprecations = datafeedConfig.getQueryDeprecations(xContentRegistry);
        if (deprecations.isEmpty()) {
            return null;
        } else {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                    "Datafeed [" + datafeedConfig.getId() + "] uses deprecated query options",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html#breaking_70_search_changes",
                    deprecations.toString());
        }
    }

    static DeprecationIssue checkDataFeedAggregations(DatafeedConfig datafeedConfig, NamedXContentRegistry xContentRegistry) {
        List<String> deprecations = datafeedConfig.getAggDeprecations(xContentRegistry);
        if (deprecations.isEmpty()) {
            return null;
        } else {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                    "Datafeed [" + datafeedConfig.getId() + "] uses deprecated aggregation options",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                            "#breaking_70_aggregations_changes", deprecations.toString());
        }
    }
}
