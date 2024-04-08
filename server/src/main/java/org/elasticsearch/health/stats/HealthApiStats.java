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

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This class tracks the health api calls and counts the statuses that have been encountered along with the unhealthy indicators and
 * diagnoses.
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

    private final Counters stats = new Counters(TOTAL_INVOCATIONS);

    public HealthApiStats() {}

    public void track(boolean verbose, GetHealthAction.Response response) {
        stats.inc(TOTAL_INVOCATIONS);
        if (verbose) {
            stats.inc(VERBOSE_TRUE);
        } else {
            stats.inc(VERBOSE_FALSE);
        }

        // The response status could be null because of a drill-down API call, in this case
        // we can use the status of the drilled down indicator
        HealthStatus status = response.getStatus() != null
            ? response.getStatus()
            : response.getIndicatorResults().stream().map(HealthIndicatorResult::status).findFirst().orElse(null);
        if (status != null) {
            stats.inc(statusLabel.apply(status));
        }

        if (status != HealthStatus.GREEN) {
            for (HealthIndicatorResult indicator : response.getIndicatorResults()) {
                if (indicator.status() != HealthStatus.GREEN) {
                    stats.inc(indicatorLabel.apply(indicator.status(), indicator.name()));
                    if (indicator.diagnosisList() != null) {
                        for (Diagnosis diagnosis : indicator.diagnosisList()) {
                            stats.inc(diagnosisLabel.apply(indicator.status(), diagnosis.definition().getUniqueId()));
                        }
                    }
                }
            }
        }
    }

    public boolean hasCounters() {
        return stats.hasCounters();
    }

    public Counters getStats() {
        return stats;
    }
}
