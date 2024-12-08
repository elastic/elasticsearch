/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
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

 * Note: If the minimum version of the cluster is not after 8.7.0 then the response will look like this:
 * {
 *   "available": false,
 *   "enabled": true
 * }
 */
public class HealthApiFeatureSetUsage extends XPackFeatureUsage {

    private final Map<String, Object> usageStats;

    public HealthApiFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        usageStats = in.readGenericMap();
    }

    public HealthApiFeatureSetUsage(boolean available, boolean enabled, @Nullable Counters stats) {
        super(XPackField.HEALTH_API, available, enabled);
        if (stats != null) {
            usageStats = stats.toMutableNestedMap();
            enrichUsageStatsWithValues(usageStats);
        } else {
            usageStats = Map.of();
        }
    }

    // This method enriches the stats map with a list of encountered values for the statuses, the indicators and the diagnoses stats.
    // Visible for testing
    @SuppressWarnings("unchecked")
    static void enrichUsageStatsWithValues(Map<String, Object> usageStats) {
        if (usageStats.containsKey("statuses")) {
            Map<String, Object> statuses = (Map<String, Object>) usageStats.get("statuses");
            if (statuses.isEmpty() == false) {
                statuses.put("values", statuses.keySet().stream().sorted().collect(Collectors.toList()));
            }
        }
        if (usageStats.containsKey("indicators")) {
            Map<String, Map<String, Object>> indicatorsByStatus = (Map<String, Map<String, Object>>) usageStats.get("indicators");
            for (String status : indicatorsByStatus.keySet()) {
                Map<String, Object> indicators = indicatorsByStatus.get(status);
                if (indicators.isEmpty() == false) {
                    indicators.put("values", indicators.keySet().stream().sorted().collect(Collectors.toList()));
                }
            }
        }
        if (usageStats.containsKey("diagnoses")) {
            Map<String, Map<String, Object>> diagnosesByStatus = (Map<String, Map<String, Object>>) usageStats.get("diagnoses");
            for (String status : diagnosesByStatus.keySet()) {
                Map<String, Object> diagnoses = diagnosesByStatus.get(status);
                if (diagnoses.isEmpty() == false) {
                    diagnoses.put("values", diagnoses.keySet().stream().sorted().collect(Collectors.toList()));
                }
            }
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_7_0;
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
