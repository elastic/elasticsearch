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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.telemetry.apm.metrics.MetricAttributes;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptySet;

public class MetricValidator {
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

    /**
     * Pattern used to validate metric names.
     * See {@code NAMING.md} for guidelines.
     */
    private static final Pattern METRIC_PATTERN = Pattern.compile(
        Strings.format(
            "es(\\.[a-z][a-z0-9_]{0,%d}){1,%d}\\.(%s)",
            MAX_SEGMENT_LENGTH - 1,
            MAX_SEGMENTS - 2,
            String.join("|", METRIC_SUFFIXES)
        )
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
     * Helper class for attribute name validation, which is only done when running with assertions enabled.
     */
    private static class Attributes {

        /**
         * Pattern used to validate attribute names, similar to {@METRIC_PATTERN} but without suffixes and using `_` as separator.
         * See {@code NAMING.md} for guidelines.
         */
        static final Pattern ATTRIBUTE_PATTERN = Pattern.compile(
            Strings.format("es(_[a-z][a-z0-9]{0,%d}){2,%d}", MAX_SEGMENT_LENGTH - 1, MAX_SEGMENTS - 2)
        );

        /**
         * Some selected attribute names defined in the OTEL attribute registry,
         * for which we don't require passing the ES specific naming pattern.
         *
         * See https://opentelemetry.io/docs/specs/semconv/registry/attributes
         */
        static final Set<String> OTEL_ATTRIBUTES = Set.of(MetricAttributes.ERROR_TYPE);

        static final Set<String> SEARCH_ATTRIBUTES = Set.of(
            "knn",
            "pit_scroll",
            "query_type",
            "response_status",
            "sort",
            "target",
            "time_range_filter_field",
            "time_range_filter_from"
        );
        static final Set<String> SEARCH_SHARD_ATTRIBUTES = Sets.addToCopy(SEARCH_ATTRIBUTES, "system_thread");

        static final Set<String> REPO_ATTRIBUTES = Set.of("operation", "purpose", "repo_name", "repo_type");
        static final Set<String> REPO_SNAPSHOT_ATTRIBUTES = Set.of("repo_name", "repo_type", "state", "stage");

        static final Set<String> REINDEX_ATTRIBUTES = Set.of("reindex_source");

        static final Set<String> RECOVERY_ATTRIBUTES = Set.of("primary", "recovery_type");

        static final Set<String> ESQL_ATTRIBUTES = Set.of("feature_name", "success");

        static final Set<String> DOWNSAMPLE_ATTRIBUTES = Set.of("status");

        static final Set<String> ALLOCATOR_NODE_ATTRIBUTES = Set.of("node_id", "node_name");

        static final Set<String> BLOB_CACHE_ATTRIBUTES = Set.of("executor", "file_extension", "reason", "source");

        static final Set<String> INFERENCE_ATTRIBUTES = Set.of("inference_source", "service", "status_code", "task_type");

        static final Set<String> ML_ATTRIBUTES = Set.of("deployment_id", "scales_to_zero");

        static final Set<String> LINKED_PROJECT_ATTRIBUTES = Set.of(
            "attempt",
            "endpoint",
            "linked_project_alias",
            "linked_project_id",
            "server_name",
            "strategy"
        );
        static final Set<String> TRANSLOG_ATTRIBUTES = Set.of("translog_blob_type", "translog_op_type");
        static final Set<String> FAILURE_STORE_ATTRIBUTES = Set.of("data_stream", "error_location", "failure_store");

        /**
         * Due to backwards compatibility some attribute names have to skip validation for specific metrics.
         *
         * Respective metrics should be expanded using attributes complying with naming guidelines.
         * In most cases this means adding a prefix {@code es_{namespace}_}. Once the new attributes
         * are available, dashboards can be migrated and old attributes removed from both the metric
         * and this skip list.
         *
         * This map associates specific metric names with sets of attributes that are allowed to skip
         * validation for those metrics only. New metrics must use properly named attributes.
         */

        // FIXME allow MetricAttributes.ERROR_TYPE
        static final Map<String, Set<String>> SKIP_VALIDATION = Map.ofEntries(
            Map.entry("es.allocator.allocations.node.disk_usage_bytes.current", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.allocations.node.forecasted_disk_usage_bytes.current", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.allocations.node.shard_count.current", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.allocations.node.undesired_shard_count.current", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.allocations.node.weight.current", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.allocations.node.write_load.current", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.balancing_round.disk_usage_bytes.histogram", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.balancing_round.shard_count.histogram", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.balancing_round.total_weight.histogram", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.balancing_round.write_load.histogram", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.desired_balance.allocations.node_disk_usage_bytes.current", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.desired_balance.allocations.node_shard_count.current", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.desired_balance.allocations.node_weight.current", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.desired_balance.allocations.node_write_load.current", ALLOCATOR_NODE_ATTRIBUTES),
            Map.entry("es.allocator.shard_write_load.distribution.current", Sets.addToCopy(ALLOCATOR_NODE_ATTRIBUTES, "percentile")),
            Map.entry("es.autoscaling.indexing.node_ingest_load.current", Sets.addToCopy(ALLOCATOR_NODE_ATTRIBUTES, "quality", "type")),
            Map.entry("es.blob_cache.miss_that_triggered_read.total", BLOB_CACHE_ATTRIBUTES),
            Map.entry("es.blob_cache.population.bytes.total", BLOB_CACHE_ATTRIBUTES),
            Map.entry("es.blob_cache.population.throughput.histogram", BLOB_CACHE_ATTRIBUTES),
            Map.entry("es.blob_cache.population.time.total", BLOB_CACHE_ATTRIBUTES),
            Map.entry("es.blob_cache.search_origin.download_took_time.total", Set.of("source")),
            Map.entry("es.blob_cache_warming.page_aligned_bytes.total", Set.of("primary", "prewarming_type")),
            Map.entry("es.breaker.trip.total", Set.of("type")),
            Map.entry("es.data_stream.ingest.documents.failure_store.total", FAILURE_STORE_ATTRIBUTES),
            Map.entry("es.data_stream.ingest.documents.rejected.total", FAILURE_STORE_ATTRIBUTES),
            Map.entry("es.data_stream.ingest.documents.total", Set.of("data_stream")),
            Map.entry("es.esql.commands.queries.total", ESQL_ATTRIBUTES),
            Map.entry("es.esql.commands.usages.total", ESQL_ATTRIBUTES),
            Map.entry("es.esql.functions.queries.total", ESQL_ATTRIBUTES),
            Map.entry("es.esql.functions.usages.total", ESQL_ATTRIBUTES),
            Map.entry("es.esql.settings.queries.total", ESQL_ATTRIBUTES),
            Map.entry("es.esql.settings.usages.total", ESQL_ATTRIBUTES),
            Map.entry("es.inference.requests.count.total", INFERENCE_ATTRIBUTES),
            Map.entry("es.inference.requests.time", INFERENCE_ATTRIBUTES),
            Map.entry("es.inference.trained_model.deployment.time", INFERENCE_ATTRIBUTES),
            Map.entry("es.ml.trained_models.adaptive_allocations.actual_number_of_allocations.current", ML_ATTRIBUTES),
            Map.entry("es.ml.trained_models.adaptive_allocations.needed_number_of_allocations.current", ML_ATTRIBUTES),
            Map.entry("es.projects.linked.connections.error.total", LINKED_PROJECT_ATTRIBUTES),
            Map.entry("es.recovery.shard.count.total", RECOVERY_ATTRIBUTES),
            Map.entry("es.recovery.shard.index.time", RECOVERY_ATTRIBUTES),
            Map.entry("es.recovery.shard.indexing_node.bytes_read.total", RECOVERY_ATTRIBUTES),
            Map.entry("es.recovery.shard.indexing_node.bytes_warmed.total", RECOVERY_ATTRIBUTES),
            Map.entry("es.recovery.shard.object_store.bytes_read.total", RECOVERY_ATTRIBUTES),
            Map.entry("es.recovery.shard.object_store.bytes_warmed.total", RECOVERY_ATTRIBUTES),
            Map.entry("es.recovery.shard.total.time", RECOVERY_ATTRIBUTES),
            Map.entry("es.recovery.shard.translog.time", RECOVERY_ATTRIBUTES),
            Map.entry("es.recovery.translog.files.size", TRANSLOG_ATTRIBUTES),
            Map.entry("es.recovery.translog.files.total", TRANSLOG_ATTRIBUTES),
            Map.entry("es.recovery.translog.operations.total", TRANSLOG_ATTRIBUTES),
            Map.entry("es.reindex.completion.total", REINDEX_ATTRIBUTES),
            Map.entry("es.reindex.duration.histogram", REINDEX_ATTRIBUTES),
            Map.entry("es.repositories.exceptions.histogram", REPO_ATTRIBUTES),
            Map.entry("es.repositories.exceptions.request_range_not_satisfied.total", REPO_ATTRIBUTES),
            Map.entry("es.repositories.exceptions.total", REPO_ATTRIBUTES),
            Map.entry("es.repositories.operations.total", REPO_ATTRIBUTES),
            Map.entry("es.repositories.operations.unsuccessful.total", REPO_ATTRIBUTES),
            Map.entry("es.repositories.requests.http_request_time.histogram", REPO_ATTRIBUTES),
            Map.entry("es.repositories.requests.total", REPO_ATTRIBUTES),
            Map.entry("es.repositories.input_stream.retry.attempts.histogram", REPO_ATTRIBUTES),
            Map.entry("es.repositories.input_stream.retry.event.total", REPO_ATTRIBUTES),
            Map.entry("es.repositories.input_stream.retry.success.total", REPO_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.blobs.uploaded.total", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.by_state.current", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.completed.total", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.create_throttling.time.total", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.duration.histogram", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.restore_throttling.time.total", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.shards.by_state.current", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.shards.completed.total", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.shards.current", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.shards.duration.histogram", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.shards.started.total", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.started.total", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.upload.bytes.total", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.upload.read_time.total", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.snapshots.upload.upload_time.total", REPO_SNAPSHOT_ATTRIBUTES),
            Map.entry("es.repositories.throttles.histogram", REPO_ATTRIBUTES),
            Map.entry("es.repositories.throttles.total", REPO_ATTRIBUTES),
            Map.entry("es.search.query.aggregations.total", Set.of("aggregation_name", "values_source")),
            Map.entry("es.search_response.response_count.total", SEARCH_ATTRIBUTES),
            Map.entry("es.search_response.took_durations.can_match.histogram", SEARCH_ATTRIBUTES),
            Map.entry("es.search_response.took_durations.dfs.histogram", SEARCH_ATTRIBUTES),
            Map.entry("es.search_response.took_durations.dfs_query.histogram", SEARCH_ATTRIBUTES),
            Map.entry("es.search_response.took_durations.fetch.histogram", SEARCH_ATTRIBUTES),
            Map.entry("es.search_response.took_durations.histogram", SEARCH_ATTRIBUTES),
            Map.entry("es.search_response.took_durations.open_pit.histogram", SEARCH_ATTRIBUTES),
            Map.entry("es.search_response.took_durations.query.histogram", SEARCH_ATTRIBUTES),
            Map.entry("es.search.shards.phases.can_match.duration.histogram", SEARCH_SHARD_ATTRIBUTES),
            Map.entry("es.search.shards.phases.dfs.duration.histogram", SEARCH_SHARD_ATTRIBUTES),
            Map.entry("es.search.shards.phases.fetch.duration.histogram", SEARCH_SHARD_ATTRIBUTES),
            Map.entry("es.search.shards.phases.query.duration.histogram", SEARCH_SHARD_ATTRIBUTES),
            Map.entry("es.tsdb.downsample.actions.shard.total", DOWNSAMPLE_ATTRIBUTES),
            Map.entry("es.tsdb.downsample.actions.total", DOWNSAMPLE_ATTRIBUTES),
            Map.entry("es.tsdb.downsample.latency.shard.histogram", DOWNSAMPLE_ATTRIBUTES),
            Map.entry("es.tsdb.downsample.latency.total.histogram", DOWNSAMPLE_ATTRIBUTES)
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
    }

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

    /**
     * Validates attribute names as per guidelines in Naming.md
     *
     * Validation will be skipped instantly if assertions are disabled.
     * If enabled, a validation failure will fail an assertion except for attributes in the skip list.
     */
    public static void assertValidAttributeNames(String metricName, Map<String, Object> attributes) {
        if (Assertions.ENABLED == false) {
            return;
        }

        if (attributes == null || attributes.isEmpty()) {
            return;
        }

        for (String attribute : attributes.keySet()) {
            validateMaxLength(attribute);

            assert Attributes.ATTRIBUTE_DENY_PATTERNS.test(attribute) == false
                : Strings.format(
                    "Attribute [%s] of [%s] is forbidden due to potential mapping conflicts or assumed high cardinality.",
                    attribute,
                    metricName
                );

            assert Attributes.OTEL_ATTRIBUTES.contains(attribute)
                || Attributes.SKIP_VALIDATION.getOrDefault(metricName, emptySet()).contains(attribute)
                || Attributes.ATTRIBUTE_PATTERN.matcher(attribute).matches()
                : Strings.format(
                    "Attribute [%s] of [%s] does not match the required naming pattern [%s], see the naming guidelines.",
                    attribute,
                    metricName,
                    Attributes.ATTRIBUTE_PATTERN
                );
        }
    }

    private static void validateMaxLength(String name) {
        if (name.length() > MAX_LENGTH) {
            throw new IllegalArgumentException(Strings.format("Name [%s] exceeded max length of [%d]", name, MAX_LENGTH));
        }
    }
}
