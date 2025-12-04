/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Assertions;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetricValidator {
    static final Set<String> METRIC_SUFFIXES = Set.of(
        "total",
        "current",
        "ratio",
        "status" /*a workaround for enums */,
        "usage",
        "size",
        "utilization",
        "histogram",
        "time"
    );
    static final int METRIC_MAX_LENGTH = 255;
    static final int METRIC_SEGMENT_MAX_LENGTH = 30;
    static final int METRIC_MAX_SEGMENTS = 10;

    private static final Pattern METRIC_PATTERN = Pattern.compile(
        Strings.format(
            "es(\\.[a-z][a-z0-9_]{0,%d}){2,%d}\\.(%s)",
            METRIC_SEGMENT_MAX_LENGTH - 1,
            METRIC_MAX_SEGMENTS - 2,
            String.join("|", METRIC_SUFFIXES)
        )
    );
    private static final Pattern ATTRIBUTE_PATTERN = Pattern.compile(
        Strings.format("es(_[a-z][a-z0-9]{0,%d}){2,%d}", METRIC_SEGMENT_MAX_LENGTH - 1, METRIC_MAX_SEGMENTS - 2)
    );

    /**
     * Due to backwards compatibility some metric names would have to skip validation.
     * This is for instance where a threadpool name is too long, or contains `-`
     * We want to allow to easily find threadpools in code base that are alerting with a metric
     * as well as find thread pools metrics in dashboards with their codebase names.
     * Renaming a threadpool name would be a breaking change.
     *
     * NOTE: only allow skipping validation if a refactor in codebase would cause a breaking change
     */
    static final Predicate<String> METRIC_SKIP_VALIDATION = Regex.simpleMatcher(
        "es.threadpool.searchable_snapshots_cache_fetch_async.*",
        "es.threadpool.searchable_snapshots_cache_prewarming.*",
        "es.threadpool.security-crypto.*"
    );

    static final Set<String> ATTRIBUTE_LEGACY_NAMES = Set.of(
        // to be populated
    );

    static final Set<String> ATTRIBUTE_GLOBAL_ACCEPTED_NAMES = Set.of(

    );

    // forbidden attributes known to cause issues due to mapping conflicts or high cardinality
    static final Predicate<String> ATTRIBUTE_DENY_PATTERNS = Regex.simpleMatcher(
        "index",
        // below field names are typically mapped to a timestamp risking mapping errors at ingest time
        // if values are not valid timestamps (which would be of high cardinality, and not desired either)
        "*.timestamp",
        "*_timestamp",
        "created",
        "*.created",
        "*.creation_date",
        "ingested",
        "*.ingested",
        "*.start",
        "*.end"
    );

    private MetricValidator() {}

    /**
     * Validates a metric name as per guidelines in Naming.md
     *
     * @param metricName metric name to be validated
     * @throws IllegalArgumentException an exception indicating an incorrect metric name
     */
    public static String validateMetricName(String metricName) {
        Objects.requireNonNull(metricName);

        if (METRIC_SKIP_VALIDATION.test(metricName)) {
            return metricName;
        }
        validateMaxMetricNameLength(metricName);

        Matcher matcher = METRIC_PATTERN.matcher(metricName);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException(
                "Metric name \""
                    + metricName
                    + "\" does not match the required pattern ["
                    + METRIC_PATTERN
                    + "], see the naming guidelines."
            );
        }
        return metricName;
    }

    public static void assertValidAttributeNames(Map<String, Object> attributes) {
        if (Assertions.ENABLED == false) {
            return;
        }

        if (attributes == null && attributes.isEmpty()) {
            return;
        }

        for (String attribute : attributes.keySet()) {
            if (ATTRIBUTE_LEGACY_NAMES.contains(attribute) || ATTRIBUTE_GLOBAL_ACCEPTED_NAMES.contains(attribute)) {
                continue;
            }
            assert ATTRIBUTE_PATTERN.matcher(attribute).matches()
                : Strings.format(
                    "Attribute name \"%s\" does not match the required pattern [%s], see the naming guidelines.",
                    attribute,
                    ATTRIBUTE_PATTERN
                );
            assert ATTRIBUTE_DENY_PATTERNS.test(attribute) == false
                : Strings.format(
                    "Attribute name [%s] is forbidden due to potential mapping conflicts or assumed high cardinality",
                    attribute
                );
        }
    }

    private static void validateMaxMetricNameLength(String metricName) {
        if (metricName.length() > METRIC_MAX_LENGTH) {
            throw new IllegalArgumentException(
                "Metric name length "
                    + metricName.length()
                    + "is longer than max metric name length:"
                    + METRIC_MAX_LENGTH
                    + " Name was: "
                    + metricName
            );
        }
    }
}
