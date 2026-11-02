/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class MlNotificationCounterTests extends ESTestCase {

    private static final String SECRET_MESSAGE = "disk watermark exceeded on node-17";
    private static final String SECRET_RESOURCE = "my-job-id";

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private Client client;
    private IndexNameExpressionResolver resolver;
    private RecordingMeterRegistry meterRegistry;
    private MlNotificationTelemetry telemetry;

    @Before
    public void setUpAuditorDependencies() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        client = mock(Client.class);
        resolver = TestIndexNameExpressionResolver.newInstance();
        meterRegistry = new RecordingMeterRegistry();
        telemetry = new MlNotificationTelemetry(meterRegistry);
    }

    @After
    public void shutDownThreadPool() {
        clusterService.close();
        terminate(threadPool);
    }

    public void testAuditAtEachLevelShouldIncrementCounterForThatLevelOnly() {
        AnomalyDetectionAuditor auditor = new AnomalyDetectionAuditor(client, clusterService, resolver, false, telemetry);

        auditor.audit(Level.INFO, SECRET_RESOURCE, SECRET_MESSAGE);
        auditor.audit(Level.WARNING, SECRET_RESOURCE, SECRET_MESSAGE);
        auditor.audit(Level.WARNING, SECRET_RESOURCE, SECRET_MESSAGE);
        auditor.audit(Level.ERROR, SECRET_RESOURCE, SECRET_MESSAGE);
        auditor.audit(Level.ERROR, SECRET_RESOURCE, SECRET_MESSAGE);
        auditor.audit(Level.ERROR, SECRET_RESOURCE, SECRET_MESSAGE);

        assertThat(countFor("info"), equalTo(1L));
        assertThat(countFor("warning"), equalTo(2L));
        assertThat(countFor("error"), equalTo(3L));
    }

    public void testInfoWarningAndErrorShortcutsShouldCountOncePerCall() {
        AnomalyDetectionAuditor auditor = new AnomalyDetectionAuditor(client, clusterService, resolver, false, telemetry);

        auditor.info(SECRET_RESOURCE, SECRET_MESSAGE);
        auditor.warning(SECRET_RESOURCE, SECRET_MESSAGE);
        auditor.error(SECRET_RESOURCE, SECRET_MESSAGE);

        assertThat(measurements(), hasSize(3));
        assertThat(countFor("info"), equalTo(1L));
        assertThat(countFor("warning"), equalTo(1L));
        assertThat(countFor("error"), equalTo(1L));
    }

    public void testSystemAuditorMessageOnlyShortcutShouldCountOncePerCall() {
        SystemAuditor auditor = new SystemAuditor(client, clusterService, resolver, telemetry);

        auditor.error(SECRET_MESSAGE);

        assertThat(measurements(), hasSize(1));
        assertThat(countFor("error"), equalTo(1L));
    }

    public void testAllAuditorTypesSharingOneTelemetryShouldAccumulateInOneSeriesPerLevel() {
        List<AbstractAuditor<?>> auditors = List.of(
            new AnomalyDetectionAuditor(client, clusterService, resolver, false, telemetry),
            new DataFrameAnalyticsAuditor(client, clusterService, resolver, false, telemetry),
            new InferenceAuditor(client, clusterService, resolver, false, telemetry),
            new SystemAuditor(client, clusterService, resolver, telemetry)
        );

        for (AbstractAuditor<?> auditor : auditors) {
            auditor.audit(Level.ERROR, SECRET_RESOURCE, SECRET_MESSAGE);
        }

        assertThat(countFor("error"), equalTo((long) auditors.size()));
    }

    public void testAttributesShouldContainOnlyLevelAndPassMetricValidatorAttributePattern() {
        // Mirrors MetricValidator.Attributes.ATTRIBUTE_PATTERN, which x-pack ML tests cannot reference (modules/apm).
        Pattern attributePattern = Pattern.compile("es(_[a-z][a-z0-9]{0,29}){2,8}");
        AnomalyDetectionAuditor auditor = new AnomalyDetectionAuditor(client, clusterService, resolver, true, telemetry);

        auditor.audit(randomFrom(Level.values()), SECRET_RESOURCE, SECRET_MESSAGE);

        List<Measurement> measurements = measurements();
        assertThat(measurements, hasSize(1));
        Map<String, Object> attributes = measurements.get(0).attributes();
        assertThat(attributes.keySet(), equalTo(Set.of(MlNotificationTelemetry.LEVEL_ATTRIBUTE)));
        assertThat(attributePattern.matcher(MlNotificationTelemetry.LEVEL_ATTRIBUTE).matches(), is(true));
        for (Object value : attributes.values()) {
            assertThat(value.toString().contains(SECRET_MESSAGE), is(false));
            assertThat(value.toString().contains(SECRET_RESOURCE), is(false));
        }
    }

    private List<Measurement> measurements() {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, MlNotificationTelemetry.NOTIFICATIONS_METRIC);
    }

    private long countFor(String level) {
        return measurements().stream()
            .filter(m -> level.equals(m.attributes().get(MlNotificationTelemetry.LEVEL_ATTRIBUTE)))
            .mapToLong(Measurement::getLong)
            .sum();
    }
}
