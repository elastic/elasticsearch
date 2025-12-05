/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    private static final Logger logger = LogManager.getLogger(MetricValidator.class);

    static final int MAX_LENGTH = 255;
    static final int MAX_SEGMENT_LENGTH = 30;
    static final int MAX_SEGMENTS = 10;

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

    private static final Pattern METRIC_PATTERN = Pattern.compile(
        Strings.format(
            "es(\\.[a-z][a-z0-9_]{0,%d}){1,%d}\\.(%s)",
            MAX_SEGMENT_LENGTH - 1,
            MAX_SEGMENTS - 2,
            String.join("|", METRIC_SUFFIXES)
        )
    );

    private static final Pattern ATTRIBUTE_PATTERN = Pattern.compile(
        Strings.format("es(_[a-z][a-z0-9]{0,%d}){2,%d}", MAX_SEGMENT_LENGTH - 1, MAX_SEGMENTS - 2)
    );

    /**
     * Due to backwards compatibility some metric names have to skip validation.
     * This is for instance where a threadpool name is too long, or contains `-`
     * We want to allow to easily find threadpools in code base that are alerting with a metric
     * as well as find thread pools metrics in dashboards with their codebase names.
     * Renaming a threadpool name would be a breaking change.
     *
     * NOTE: only allow skipping validation if a refactor in codebase would cause a breaking change
     */
    static final Predicate<String> METRIC_SKIP_VALIDATION = Regex.simpleMatcher(
        "es.thread_pool.searchable_snapshots_cache_fetch_async.*",
        "es.thread_pool.searchable_snapshots_cache_prewarming.*",
        "es.thread_pool.security-crypto.*",
        "es.thread_pool.security-token-key.*"
    );

    /**
     * Due to backwards compatibility some attribute names have to skip validation.
     *
     * Respective metrics should be expanded using attributes complying with naming guidelines.
     * In most cases this means adding a prefix {@code es_{namespace}_}. Once the new attributes
     * are available, dashboards can be migrated and old attributes removed from both the metric
     * and this skip list.
     */
    static final Set<String> ATTRIBUTE_SKIP_VALIDATION = Set.of(
        "action",
        "aggregation_name",
        "attempt",
        "backfill-type",
        "channel",
        "data_stream",
        "deployment_id",
        "endpoint",
        "error_location",
        "error_type",
        "executor",
        "failure_store",
        "feature_name",
        "file_extension",
        "inference_source",
        "knn",
        "linked_project_alias",
        "linked_project_id",
        "node_id",
        "node_name",
        "operation",
        "pit_scroll",
        "prewarming_type",
        "primary",
        "purpose",
        "query_type",
        "reason",
        "recovery_type",
        "reindex_source",
        "repo_name",
        "response_status",
        "server_name",
        "scales_to_zero",
        "sort",
        "source",
        "stage",
        "state",
        "status",
        "strategy",
        "success",
        "system_thread",
        "target",
        "task_type",
        "time_range_filter_field",
        "time_range_filter_from",
        "translog_blob_type",
        "translog_op_type",
        "type",
        "values_source",
        "status_code"
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
        validateMaxLength(metricName);

        Matcher matcher = METRIC_PATTERN.matcher(metricName);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Metric name [%s] does not match the required naming pattern [%s], see the naming guidelines.",
                    metricName,
                    METRIC_PATTERN
                )
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
            validateMaxLength(attribute);

            boolean isValid = ATTRIBUTE_PATTERN.matcher(attribute).matches();
            boolean isDenied = ATTRIBUTE_DENY_PATTERNS.test(attribute);
            if (isValid && isDenied == false) {
                continue;
            }

            var message = isDenied
                ? Strings.format(
                    "Attribute name [%s] is forbidden due to potential mapping conflicts or assumed high cardinality.",
                    attribute
                )
                : Strings.format(
                    "Attribute name [%s] does not match the required naming pattern [%s], see the naming guidelines.",
                    attribute,
                    ATTRIBUTE_PATTERN
                );
            // we cannot log a deprecation here, that would fail too many tests
            logger.error(message);
            assert ATTRIBUTE_SKIP_VALIDATION.contains(attribute) : message;
        }
    }

    private static void validateMaxLength(String name) {
        if (name.length() > MAX_LENGTH) {
            throw new IllegalArgumentException(Strings.format("Name [%s] exceeded max length of [%d]", name, MAX_LENGTH));
        }
    }
}
