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

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PlanTelemetryManagerTests extends ESTestCase {

    public void testPublishQueryRecordsOneQueryWithClosedSetAttributes() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PlanTelemetryManager manager = new PlanTelemetryManager(registry);
        QueryMetric client = randomFrom(QueryMetric.values());
        boolean success = randomBoolean();

        manager.publishQuery(new QueryCaller(client), success);

        List<Measurement> measurements = queryMeasurements(registry);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.getFirst().getLong(), equalTo(1L));
        assertThat(
            measurements.getFirst().attributes(),
            equalTo(Map.of(PlanTelemetryManager.CLIENT, client.toString(), PlanTelemetryManager.QUERY_SUCCESS, success))
        );
    }

    public void testPublishQueryCountsEachQuery() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PlanTelemetryManager manager = new PlanTelemetryManager(registry);
        int queries = between(2, 10);
        for (int i = 0; i < queries; i++) {
            manager.publishQuery(new QueryCaller(QueryMetric.KIBANA), true);
        }
        assertThat(Measurement.combine(queryMeasurements(registry)).getFirst().getLong(), equalTo((long) queries));
    }

    /**
     * The APM registry rejects, with an assertion that stops the node, attribute names outside the {@code es_*} naming rule
     * ({@code MetricValidator}). The recording registry used in tests does not check names, so this pins the rule here.
     */
    public void testQueryAttributeNamesFollowTheApmNamingRule() {
        Pattern apmAttributeName = Pattern.compile("es(_[a-z][a-z0-9]{0,29}){2,8}");
        for (String attribute : List.of(PlanTelemetryManager.CLIENT, PlanTelemetryManager.QUERY_SUCCESS)) {
            assertTrue(attribute, apmAttributeName.matcher(attribute).matches());
        }
    }

    private static List<Measurement> queryMeasurements(RecordingMeterRegistry registry) {
        return registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, PlanTelemetryManager.QUERY_METRICS);
    }
}
