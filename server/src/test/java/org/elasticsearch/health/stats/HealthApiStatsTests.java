/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.stats;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.hamcrest.Matchers.equalTo;

public class HealthApiStatsTests extends ESTestCase {

    public void testEmptyCounters() {
        HealthApiStats healthApiStats = new HealthApiStats();
        assertThat(healthApiStats.getStats().get("invocations.total"), equalTo(0L));
        Map<String, Object> metricMap = healthApiStats.getStats().toMutableNestedMap();
        assertThat(metricMap.containsKey("statuses"), equalTo(false));
        assertThat(metricMap.containsKey("indicators"), equalTo(false));
        assertThat(metricMap.containsKey("diagnoses"), equalTo(false));
    }

    @SuppressWarnings("unchecked")
    public void testCounters() {
        HealthApiStats healthApiStats = new HealthApiStats();

        {
            // All green
            var indicators = List.of(
                createHealthIndicatorResult("network_latency", GREEN),
                createHealthIndicatorResult("shards_availability", GREEN)
            );
            GetHealthAction.Response response = new GetHealthAction.Response(ClusterName.DEFAULT, indicators, true);

            healthApiStats.track(false, response);
            assertThat(healthApiStats.getStats().get("invocations.total"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("invocations.verbose_false"), equalTo(1L));
            expectThrows(IllegalArgumentException.class, () -> healthApiStats.getStats().get("invocations.verbose_true"));
            assertThat(healthApiStats.getStats().get("statuses.green"), equalTo(1L));
            Map<String, Object> metricMap = healthApiStats.getStats().toMutableNestedMap();
            assertThat(((Map<String, Object>) metricMap.get("statuses")).size(), equalTo(1));
            assertThat(metricMap.containsKey("indicators"), equalTo(false));
            assertThat(metricMap.containsKey("diagnoses"), equalTo(false));
        }
        {
            // Yellow no diagnosis
            var indicators = List.of(
                createHealthIndicatorResult("network_latency", GREEN),
                createHealthIndicatorResult("shards_availability", YELLOW)
            );
            GetHealthAction.Response response = new GetHealthAction.Response(ClusterName.DEFAULT, indicators, true);

            healthApiStats.track(true, response);
            assertThat(healthApiStats.getStats().get("invocations.total"), equalTo(2L));
            assertThat(healthApiStats.getStats().get("invocations.verbose_true"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("invocations.verbose_false"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("statuses.green"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("statuses.yellow"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("indicators.yellow.shards_availability"), equalTo(1L));
            Map<String, Object> metricMap = healthApiStats.getStats().toMutableNestedMap();
            assertThat(metricMap.containsKey("diagnoses"), equalTo(false));
        }
        {
            // Yellow and Red with diagnoses
            var indicators = List.of(
                createHealthIndicatorResult("network_latency", GREEN),
                createHealthIndicatorResult("disk", YELLOW, "add_disk_capacity_data_nodes"),
                createHealthIndicatorResult("shards_availability", RED, "primary_unassigned")
            );
            GetHealthAction.Response response = new GetHealthAction.Response(ClusterName.DEFAULT, indicators, true);

            healthApiStats.track(true, response);
            assertThat(healthApiStats.getStats().get("invocations.total"), equalTo(3L));
            assertThat(healthApiStats.getStats().get("invocations.verbose_true"), equalTo(2L));
            assertThat(healthApiStats.getStats().get("invocations.verbose_false"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("statuses.green"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("statuses.yellow"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("indicators.yellow.shards_availability"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("indicators.yellow.disk"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("statuses.red"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("indicators.red.shards_availability"), equalTo(1L));
            assertThat(
                healthApiStats.getStats().get("diagnoses.yellow.elasticsearch:health:disk:diagnosis:add_disk_capacity_data_nodes"),
                equalTo(1L)
            );
            assertThat(
                healthApiStats.getStats().get("diagnoses.red.elasticsearch:health:shards_availability:diagnosis:primary_unassigned"),
                equalTo(1L)
            );
        }
        {
            // Increment counters
            var indicators = List.of(
                createHealthIndicatorResult("network_latency", GREEN),
                createHealthIndicatorResult("disk", RED, "add_disk_capacity_data_nodes"),
                createHealthIndicatorResult("shards_availability", RED, "primary_unassigned")
            );
            GetHealthAction.Response response = new GetHealthAction.Response(ClusterName.DEFAULT, indicators, true);

            healthApiStats.track(true, response);
            assertThat(healthApiStats.getStats().get("invocations.total"), equalTo(4L));
            assertThat(healthApiStats.getStats().get("invocations.verbose_true"), equalTo(3L));
            assertThat(healthApiStats.getStats().get("invocations.verbose_false"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("statuses.green"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("statuses.yellow"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("indicators.yellow.disk"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("indicators.yellow.shards_availability"), equalTo(1L));
            assertThat(healthApiStats.getStats().get("statuses.red"), equalTo(2L));
            assertThat(healthApiStats.getStats().get("indicators.red.shards_availability"), equalTo(2L));
            assertThat(healthApiStats.getStats().get("indicators.red.disk"), equalTo(1L));
            assertThat(
                healthApiStats.getStats().get("diagnoses.yellow.elasticsearch:health:disk:diagnosis:add_disk_capacity_data_nodes"),
                equalTo(1L)
            );
            assertThat(
                healthApiStats.getStats().get("diagnoses.red.elasticsearch:health:shards_availability:diagnosis:primary_unassigned"),
                equalTo(2L)
            );
            assertThat(
                healthApiStats.getStats().get("diagnoses.red.elasticsearch:health:disk:diagnosis:add_disk_capacity_data_nodes"),
                equalTo(1L)
            );
        }
    }

    private HealthIndicatorResult createHealthIndicatorResult(String indicator, HealthStatus status, String... diagnosisIds) {
        return new HealthIndicatorResult(
            indicator,
            status,
            null,
            null,
            null,
            Arrays.stream(diagnosisIds)
                .map(diagnosisId -> new Diagnosis(new Diagnosis.Definition(indicator, diagnosisId, "", "", ""), null))
                .toList()
        );
    }
}
