/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.fetch.OpenFetchContexts;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhaseOutcomes;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The usage counters and the APM counter of the decisions of the planner about the fetch phase.
 */
public class FetchPhaseMetricsTests extends ESTestCase {

    @Before
    public void checkTheFetchPhaseIsAvailable() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
    }

    public void testTheUsageCountersCountEachQueryByItsOutcome() {
        Metrics metrics = new Metrics(new EsqlFunctionRegistry(), true, false);
        metrics.incFetchPhase(Outcome.APPLIED);
        metrics.incFetchPhase(Outcome.APPLIED);
        metrics.incFetchPhase(Outcome.INELIGIBLE_SHAPE);

        Counters stats = metrics.stats();
        assertThat(stats.get("fetch_phase.applied"), equalTo(2L));
        assertThat(stats.get("fetch_phase.ineligible_shape"), equalTo(1L));
        assertThat(stats.get("fetch_phase.disabled_pragma"), equalTo(0L));
    }

    /**
     * Only the outcomes {@code EXPLAIN} reports have a usage counter, so a cluster that never asked for the fetch phase
     * reports zeros.
     */
    public void testOnlyTheOutcomesThatExplainReportsHaveAUsageCounter() {
        Map<?, ?> fetchPhase = (Map<?, ?>) new Metrics(new EsqlFunctionRegistry(), true, false).stats().toNestedMap().get("fetch_phase");
        String[] expected = Arrays.stream(Outcome.values())
            .filter(FetchPhaseOutcomes::isExplained)
            .map(outcome -> outcome.name().toLowerCase(Locale.ROOT))
            .toArray(String[]::new);
        assertThat(fetchPhase.keySet(), containsInAnyOrder((Object[]) expected));
    }

    /**
     * A node refuses to start with a metric whose name breaks the naming rules of APM, and with assertions on it fails the
     * first measurement whose attribute names break them. The test registries accept any name.
     */
    public void testTheMetricNamesFollowTheApmNamingRules() {
        // the patterns MetricValidator of the apm module checks
        Pattern apmName = Pattern.compile(
            "es(\\.[a-z][a-z0-9_]{0,29}){1,8}\\.(utilization|status|total|current|duration|ratio|time|histogram|size|usage)"
        );
        Pattern apmAttribute = Pattern.compile("es(_[a-z][a-z0-9]{0,29}){2,8}");
        for (String name : List.of(PlanTelemetryManager.FETCH_PHASE_METRICS, OpenFetchContexts.OPEN_CONTEXTS_GAUGE)) {
            assertTrue(name, apmName.matcher(name).matches());
        }
        for (String attribute : List.of(PlanTelemetryManager.FETCH_PHASE_OUTCOME, PlanTelemetryManager.FETCH_PHASE_QUERY_SUCCESS)) {
            assertTrue(attribute, apmAttribute.matcher(attribute).matches());
        }
    }

    public void testTheApmCounterCountsAQueryWithItsOutcomeAndSuccess() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        new PlanTelemetryManager(registry).publishFetchPhase(Outcome.APPLIED, false);

        List<Measurement> measurements = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, PlanTelemetryManager.FETCH_PHASE_METRICS);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.getFirst().getLong(), equalTo(1L));
        assertThat(
            measurements.getFirst().attributes(),
            equalTo(Map.of(PlanTelemetryManager.FETCH_PHASE_OUTCOME, "applied", PlanTelemetryManager.FETCH_PHASE_QUERY_SUCCESS, false))
        );
    }
}
