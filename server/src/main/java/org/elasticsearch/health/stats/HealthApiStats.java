/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.stats;

import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.core.Strings;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This class tracks the health api calls and counts the statuses that have been encountered along with the unhealthy indicators and
 * diagnoses. An example of what the stats look like is seen below. The stats are exposed via the xpack usage api for telemetry.
 * {
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
 *       "values": ["elasticsearch:health:disk:add_disk_capacity_data_nodes"]
 *     }
 *   }
 * }
 */
public class HealthApiStats {

    private static final String TOTAL_INVOCATIONS = "invocations.total";
    private static final String VERBOSE_TRUE = "invocations.verbose_true";
    private static final String VERBOSE_FALSE = "invocations.verbose_false";
    private final Function<HealthStatus, String> statusLabel = status -> Strings.format("statuses.%s", status.xContentValue());
    private final BiFunction<HealthStatus, String, String> indicatorLabel = (status, indicator) -> Strings.format(
        "indicators.%s.%s",
        status.xContentValue(),
        indicator
    );
    private final BiFunction<HealthStatus, String, String> diagnosisLabel = (status, diagnosis) -> Strings.format(
        "diagnoses.%s.%s",
        status.xContentValue(),
        diagnosis
    );

    private final Set<HealthStatus> statuses = ConcurrentHashMap.newKeySet();
    private final Map<HealthStatus, Set<String>> indicators = new ConcurrentHashMap<>();
    private final Map<HealthStatus, Set<String>> diagnoses = new ConcurrentHashMap<>();
    private final Counters counters = new Counters(TOTAL_INVOCATIONS);

    public HealthApiStats() {}

    public void track(boolean verbose, GetHealthAction.Response response) {
        counters.inc(TOTAL_INVOCATIONS);
        if (verbose) {
            counters.inc(VERBOSE_TRUE);
        } else {
            counters.inc(VERBOSE_FALSE);
        }
        HealthStatus status = response.getStatus() != null
            ? response.getStatus()
            : response.getIndicators().stream().map(HealthIndicatorResult::status).findFirst().orElse(null);
        if (status != null) {
            counters.inc(statusLabel.apply(status));
            statuses.add(status);
        }

        if (status != HealthStatus.GREEN) {
            for (HealthIndicatorResult indicator : response.getIndicators()) {
                if (indicator.status() != HealthStatus.GREEN) {
                    counters.inc(indicatorLabel.apply(indicator.status(), indicator.name()));
                    indicators.computeIfAbsent(indicator.status(), k -> ConcurrentHashMap.newKeySet()).add(indicator.name());
                    if (indicator.diagnosisList() != null) {
                        for (Diagnosis diagnosis : indicator.diagnosisList()) {
                            counters.inc(diagnosisLabel.apply(indicator.status(), diagnosis.definition().getUniqueId()));
                            diagnoses.computeIfAbsent(indicator.status(), k -> ConcurrentHashMap.newKeySet())
                                .add(diagnosis.definition().getUniqueId());
                        }
                    }
                }
            }
        }
    }

    public boolean hasCounters() {
        return counters.hasCounters();
    }

    public Counters getCounters() {
        return counters;
    }

    public Map<HealthStatus, Set<String>> getIndicators() {
        return Map.copyOf(indicators);
    }

    public Map<HealthStatus, Set<String>> getDiagnoses() {
        return Map.copyOf(diagnoses);
    }

    public Set<HealthStatus> getStatuses() {
        return Set.copyOf(statuses);
    }
}
