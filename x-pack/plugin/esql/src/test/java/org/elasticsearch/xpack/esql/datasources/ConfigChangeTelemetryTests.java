/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceUsageAccumulator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ConfigChangeTelemetryTests extends ESTestCase {

    public void testTypeTokenFoldsAndClamps() {
        assertThat(ConfigChangeTelemetry.typeToken("s3"), equalTo("s3"));
        assertThat(ConfigChangeTelemetry.typeToken("GCS"), equalTo("gcs"));
        assertThat(ConfigChangeTelemetry.typeToken("file"), equalTo("unknown"));
        assertThat(ConfigChangeTelemetry.typeToken("local"), equalTo("local"));
        assertThat(ConfigChangeTelemetry.typeToken("test"), equalTo("unknown"));
        assertThat(ConfigChangeTelemetry.typeToken(null), equalTo("unknown"));
    }

    public void testRejectedReasonMapping() {
        assertThat(ConfigChangeTelemetry.rejectedReason(new ValidationException()), equalTo("validation"));
        assertThat(ConfigChangeTelemetry.rejectedReason(new ResourceNotFoundException("x")), equalTo("not_found"));
        assertThat(ConfigChangeTelemetry.rejectedReason(new ResourceAlreadyExistsException("x")), equalTo("already_exists"));
        assertThat(ConfigChangeTelemetry.rejectedReason(new UnknownDataSourceTypeException("nope")), equalTo("unknown_type"));
        assertThat(ConfigChangeTelemetry.rejectedReason(new MaxDataSourcesCountException(1)), equalTo("max_count"));
        assertThat(ConfigChangeTelemetry.rejectedReason(new MaxDatasetsCountException(1)), equalTo("max_count"));
        assertThat(
            ConfigChangeTelemetry.rejectedReason(new ElasticsearchStatusException("enc", RestStatus.SERVICE_UNAVAILABLE)),
            equalTo("unavailable")
        );
        assertThat(
            ConfigChangeTelemetry.rejectedReason(new ElasticsearchStatusException("conflict", RestStatus.CONFLICT)),
            equalTo("has_dependents")
        );
        assertThat(ConfigChangeTelemetry.rejectedReason(new IllegalArgumentException("bad mapping")), equalTo("validation"));
    }

    public void testPublishFailureIsNotARejection() {
        Exception publish = new FailedToCommitClusterStateException("retry");
        assertTrue(MasterService.isPublishFailureException(publish));
        assertThat(ConfigChangeTelemetry.rejectedReason(publish), nullValue());
        assertThat(ConfigChangeTelemetry.rejectedReason(new NotMasterException("not master")), nullValue());
    }

    public void testRecordRejectedPinsEachReasonOnBothSinks() {
        recordAndAssert(new ValidationException(), "validation");
        recordAndAssert(new ResourceNotFoundException("x"), "not_found");
        recordAndAssert(new ResourceAlreadyExistsException("x"), "already_exists");
        recordAndAssert(new UnknownDataSourceTypeException("nope"), "unknown_type");
        recordAndAssert(new MaxDataSourcesCountException(1), "max_count");
        recordAndAssert(new MaxDatasetsCountException(2), "max_count");
        recordAndAssert(new ElasticsearchStatusException("enc", RestStatus.SERVICE_UNAVAILABLE), "unavailable");
        recordAndAssert(new ElasticsearchStatusException("conflict", RestStatus.CONFLICT), "has_dependents");
        recordAndAssert(new IllegalArgumentException("bad mapping"), "validation");
    }

    public void testRecordRejectedSkipsPublishFailure() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry, acc);
        ConfigChangeTelemetry.recordRejected(metrics, ConfigChangeTelemetry.KIND_DATASOURCE, "s3", new NotMasterException("not master"));
        assertThat(acc.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_REJECTED), equalTo(0L));
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.CONFIG_CHANGES_TOTAL),
            hasSize(0)
        );
    }

    private static void recordAndAssert(Exception e, String reason) {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry, acc);
        ConfigChangeTelemetry.recordRejected(metrics, ConfigChangeTelemetry.KIND_DATASOURCE, "s3", e);
        assertThat(acc.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_REJECTED), equalTo(1L));
        Measurement m = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.CONFIG_CHANGES_TOTAL)
            .get(0);
        assertThat(m.attributes().get(ExternalSourceMetrics.REASON_ATTRIBUTE), equalTo(reason));
        assertThat(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));
        assertThat(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE), equalTo("rejected"));
    }
}
