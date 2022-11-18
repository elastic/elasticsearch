/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Models the health api usage section in the XPack usage response. A sample response would look like this:
 * {
 *   "enabled": true,
 *   "available": true,
 *   "invocations": {
 *     "total": 22,
 *     "verbose_true": 12,
 *     "verbose_false": 10
 *   },
 *   "statuses": {
 *     "green": 10,
 *     "yellow": 4,
 *     "red": 8,
 *     "values": ["green", "yellow", "red"]
 *   },
 *   "indicators": {
 *     "red" : {
 *       "master_stability": 2,
 *       "ilm":2,
 *       "slm": 4,
 *       "values": ["master_stability", "ilm", "slm"]
 *     },
 *     "yellow": {
 *       "disk": 1,
 *       "shards_availability": 1,
 *       "master_stability": 2,
 *       "values": ["disk", "shards_availability", "master_stability"]
 *     }
 *   },
 *   "diagnoses": {
 *     "red": {
 *       "elasticsearch:health:shards_availability:primary_unassigned": 1,
 *       "elasticsearch:health:disk:add_disk_capacity_master_nodes": 3,
 *       "values": [
 *         "elasticsearch:health:shards_availability:primary_unassigned",
 *         "elasticsearch:health:disk:add_disk_capacity_master_nodes"
 *       ]
 *     },
 *     "yellow": {
 *       "elasticsearch:health:disk:add_disk_capacity_data_nodes": 1,
 *       "values": [""elasticsearch:health:disk:add_disk_capacity_data_nodes"]
 *     }
 *   }
 * }
 */
public class HealthApiFeatureSetUsage extends XPackFeatureSet.Usage {

    private final Map<String, Object> usageStats;

    public HealthApiFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        usageStats = in.readMap();
    }

    public HealthApiFeatureSetUsage(
        boolean available,
        boolean enabled,
        Counters stats,
        Set<HealthStatus> statuses,
        Map<HealthStatus, Set<String>> indicators,
        Map<HealthStatus, Set<String>> diagnoses
    ) {
        super(XPackField.HEALTH_API, available, enabled);
        usageStats = stats.toNestedMap();
        addValues(usageStats, List.of("statuses"), statuses.stream().map(HealthStatus::xContentValue).collect(Collectors.toSet()));
        for (HealthStatus status : indicators.keySet()) {
            addValues(usageStats, List.of("indicators", status.xContentValue()), indicators.get(status));
        }
        for (HealthStatus status : diagnoses.keySet()) {
            addValues(usageStats, List.of("diagnoses", status.xContentValue()), diagnoses.get(status));
        }
    }

    @SuppressWarnings("unchecked")
    private static void addValues(Map<String, Object> map, List<String> path, Set<String> values) {
        if (values.isEmpty()) {
            return;
        }
        Map<String, Object> currentMap = map;
        for (String field : path) {
            currentMap = (Map<String, Object>) currentMap.computeIfAbsent(field, k -> new HashMap<>());
        }
        currentMap.put("values", values.stream().sorted().toList());
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_7_0;
    }

    public Map<String, Object> stats() {
        return usageStats;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        for (Map.Entry<String, Object> entry : usageStats.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(usageStats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HealthApiFeatureSetUsage that = (HealthApiFeatureSetUsage) o;
        return Objects.equals(usageStats, that.usageStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(usageStats);
    }
}
