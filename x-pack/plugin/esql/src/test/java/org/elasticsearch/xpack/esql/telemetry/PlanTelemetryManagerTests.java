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
import org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.Feature;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PlanTelemetryManagerTests extends ESTestCase {

    public void testPublishQueryRecordsOneQueryWithClosedSetAttributes() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PlanTelemetryManager manager = new PlanTelemetryManager(registry);
        QueryMetric client = randomValueOtherThan(QueryMetric.REST, () -> randomFrom(QueryMetric.values()));
        QueryEntryPoint entryPoint = randomFrom(QueryEntryPoint.values());
        LeadingCommand leadingCommand = randomFrom(LeadingCommand.values());
        boolean success = randomBoolean();

        manager.publishQuery(new QueryCaller(client, null), entryPoint, null, leadingCommand, success);

        List<Measurement> measurements = queryMeasurements(registry);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.getFirst().getLong(), equalTo(1L));
        assertThat(
            measurements.getFirst().attributes(),
            equalTo(
                Map.of(
                    PlanTelemetryManager.CLIENT,
                    client.toString(),
                    PlanTelemetryManager.ENTRY_POINT,
                    entryPoint.toString(),
                    PlanTelemetryManager.LEADING_COMMAND,
                    leadingCommand.toString(),
                    PlanTelemetryManager.QUERY_SUCCESS,
                    success
                )
            )
        );
    }

    public void testPublishQueryAddsTheDirectClient() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PlanTelemetryManager manager = new PlanTelemetryManager(registry);
        DirectClient directClient = randomFrom(DirectClient.values());

        manager.publishQuery(new QueryCaller(QueryMetric.REST, null), QueryEntryPoint.QUERY, directClient, LeadingCommand.FROM, true);

        List<Measurement> measurements = queryMeasurements(registry);
        assertThat(measurements, hasSize(1));
        assertThat(
            measurements.getFirst().attributes(),
            equalTo(
                Map.of(
                    PlanTelemetryManager.CLIENT,
                    "rest",
                    PlanTelemetryManager.ENTRY_POINT,
                    "query",
                    PlanTelemetryManager.DIRECT_CLIENT,
                    directClient.toString(),
                    PlanTelemetryManager.LEADING_COMMAND,
                    "from",
                    PlanTelemetryManager.QUERY_SUCCESS,
                    true
                )
            )
        );
    }

    public void testPublishQueryAddsTheKibanaFeatureAndInteraction() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PlanTelemetryManager manager = new PlanTelemetryManager(registry);
        Feature feature = randomFrom(Feature.values());

        manager.publishQuery(new QueryCaller(QueryMetric.KIBANA, feature), QueryEntryPoint.QUERY_ASYNC, null, LeadingCommand.FROM, false);

        List<Measurement> measurements = queryMeasurements(registry);
        assertThat(measurements, hasSize(1));
        assertThat(
            measurements.getFirst().attributes(),
            equalTo(
                Map.of(
                    PlanTelemetryManager.CLIENT,
                    "kibana",
                    PlanTelemetryManager.ENTRY_POINT,
                    "query_async",
                    PlanTelemetryManager.KIBANA_FEATURE,
                    feature.toString(),
                    PlanTelemetryManager.KIBANA_INTERACTION,
                    feature.interaction().toString(),
                    PlanTelemetryManager.LEADING_COMMAND,
                    "from",
                    PlanTelemetryManager.QUERY_SUCCESS,
                    false
                )
            )
        );
    }

    public void testPublishQueryCountsEachQuery() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PlanTelemetryManager manager = new PlanTelemetryManager(registry);
        int queries = between(2, 10);
        QueryCaller caller = new QueryCaller(QueryMetric.KIBANA, randomFrom(Feature.values()));
        for (int i = 0; i < queries; i++) {
            manager.publishQuery(caller, QueryEntryPoint.QUERY, null, LeadingCommand.FROM, true);
        }
        assertThat(Measurement.combine(queryMeasurements(registry)).getFirst().getLong(), equalTo((long) queries));
    }

    /**
     * The APM registry rejects, with an assertion that stops the node, attribute names outside the {@code es_*} naming rule
     * ({@code MetricValidator}). The recording registry used in tests does not check names, so this pins the rule here.
     */
    public void testQueryAttributeNamesFollowTheApmNamingRule() {
        Pattern apmAttributeName = Pattern.compile("es(_[a-z][a-z0-9]{0,29}){2,8}");
        for (String attribute : List.of(
            PlanTelemetryManager.CLIENT,
            PlanTelemetryManager.ENTRY_POINT,
            PlanTelemetryManager.DIRECT_CLIENT,
            PlanTelemetryManager.KIBANA_FEATURE,
            PlanTelemetryManager.KIBANA_INTERACTION,
            PlanTelemetryManager.LEADING_COMMAND,
            PlanTelemetryManager.QUERY_SUCCESS
        )) {
            assertTrue(attribute, apmAttributeName.matcher(attribute).matches());
        }
    }

    private static List<Measurement> queryMeasurements(RecordingMeterRegistry registry) {
        return registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, PlanTelemetryManager.QUERY_METRICS);
    }
}
