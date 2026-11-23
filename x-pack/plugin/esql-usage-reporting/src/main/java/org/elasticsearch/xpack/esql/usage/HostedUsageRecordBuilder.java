/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Builds the usage records of hosted (stateful) deployments: one record per outcome with usage in a drain, so one per node and
 * drain for billed usage, and one more for each kind of exempt usage.
 * <p>
 * Everything that makes a retry safe is decided here. The id is derived only from the node, the usage type, the drain time and the
 * outcome, so the same drain always yields the same ids and the Usage API sees a re-sent record as a duplicate, never as more usage;
 * after a restart the node id is usually the same (it is persisted in the data path), and ids stay distinct because the drain time
 * differs. Usage of queries that did not succeed never shares a record with billed usage, since a record has a single exemption
 * flag. The quantity is the whole milliseconds of the sum, and the remainder is dropped
 * rather than rounded up: when in doubt the customer is under-billed, never over-billed.
 */
public final class HostedUsageRecordBuilder implements UsageRecordBuilder {

    private static final Logger logger = LogManager.getLogger(HostedUsageRecordBuilder.class);

    /** Fixed width, so that ids and timestamps sort and compare as text whatever the sub-second part is. */
    static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    static final String SOURCE_ID_PREFIX = "es-";

    // Keys of usage.metadata: the counted components in nanoseconds, so that the millisecond quantity can be explained afterwards.
    static final String EXECUTION_CPU_NANOS = "cpu_nanos";
    static final String READ_CPU_NANOS = "read_cpu_nanos";
    static final String PLANNING_CPU_NANOS = "planning_cpu_nanos";
    static final String SPLIT_DISCOVERY_CPU_NANOS = "split_discovery_cpu_nanos";

    private static final long NANOS_PER_MILLI = 1_000_000L;

    /** The problem logged last, so that a node whose identity stays incomplete logs it once and not at every drain. */
    private final AtomicReference<String> lastLoggedProblem = new AtomicReference<>();

    @Override
    public List<UsageRecord> build(UsageSnapshot snapshot, SourceIdentity identity, Duration period, Instant creationTime) {
        if (identity.isComplete() == false) {
            reportIncompleteIdentity(identity);
            return List.of();
        }
        lastLoggedProblem.set(null);

        Instant drainTime = snapshot.drainTime().truncatedTo(ChronoUnit.MILLIS);
        String sourceId = SOURCE_ID_PREFIX + identity.nodeId();
        String baseId = sourceId + "_" + UsageTypes.EXTERNAL_SOURCES_CPU + "_" + TIMESTAMP_FORMAT.format(drainTime);
        List<UsageRecord> records = new ArrayList<>();
        // Outcome order, not map order, so that the records come out in the same order for the same snapshot.
        for (Outcome outcome : Outcome.values()) {
            CpuComponents drained = snapshot.byOutcome().get(outcome);
            if (drained == null) {
                continue;
            }
            // A negative component is an anomaly of the metrics: it is dropped before summing, so that the quantity and the
            // metadata that explains it agree, and it never reduces what another component counted.
            CpuComponents components = new CpuComponents(
                Math.max(0, drained.planningNanos()),
                Math.max(0, drained.splitDiscoveryNanos()),
                Math.max(0, drained.executionNanos()),
                Math.max(0, drained.readNanos())
            );
            long quantityMillis = components.totalNanos() / NANOS_PER_MILLI;
            if (quantityMillis <= 0) {
                continue;
            }
            UsageRecord.Exemption exemption = exemption(outcome);
            records.add(
                new UsageRecord(
                    exemption == null ? baseId : baseId + "_" + exemption.reason(),
                    drainTime,
                    creationTime.truncatedTo(ChronoUnit.MILLIS),
                    UsageTypes.EXTERNAL_SOURCES_CPU,
                    quantityMillis,
                    period.toSeconds(),
                    metadata(components),
                    exemption,
                    identity
                )
            );
        }
        return records;
    }

    /** Null for billed usage. */
    private static UsageRecord.Exemption exemption(Outcome outcome) {
        return switch (outcome) {
            case SUCCESS -> null;
            case FAILED -> new UsageRecord.Exemption("failed");
            case CANCELLED -> new UsageRecord.Exemption("cancelled");
        };
    }

    /** The components that are not zero, in a fixed order. */
    private static Map<String, String> metadata(CpuComponents components) {
        Map<String, String> metadata = new LinkedHashMap<>();
        putIfNotZero(metadata, EXECUTION_CPU_NANOS, components.executionNanos());
        putIfNotZero(metadata, READ_CPU_NANOS, components.readNanos());
        putIfNotZero(metadata, PLANNING_CPU_NANOS, components.planningNanos());
        putIfNotZero(metadata, SPLIT_DISCOVERY_CPU_NANOS, components.splitDiscoveryNanos());
        return Collections.unmodifiableMap(metadata);
    }

    private static void putIfNotZero(Map<String, String> metadata, String key, long nanos) {
        if (nanos != 0) {
            metadata.put(key, Long.toString(nanos));
        }
    }

    private void reportIncompleteIdentity(SourceIdentity identity) {
        List<String> missing = new ArrayList<>();
        addIfMissing(missing, "node id", identity.nodeId());
        addIfMissing(missing, "deployment id", identity.deploymentId());
        addIfMissing(missing, "cluster id", identity.clusterId());
        addIfMissing(missing, "provider", identity.provider());
        addIfMissing(missing, "region", identity.region());
        String problem = String.join(", ", missing);
        if (problem.equals(lastLoggedProblem.getAndSet(problem)) == false) {
            logger.error("usage records are not built because the identity of this node is incomplete, missing: [{}]", problem);
        }
    }

    private static void addIfMissing(List<String> missing, String name, String value) {
        if (value == null || value.isBlank()) {
            missing.add(name);
        }
    }
}
