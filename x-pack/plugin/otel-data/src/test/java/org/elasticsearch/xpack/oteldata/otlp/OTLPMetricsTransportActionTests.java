/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.Metric;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OTLPMetricsTransportActionTests extends AbstractOTLPTransportActionTests {

    private OTLPMetricsTransportAction metricsAction;
    private ClusterSettings clusterSettings;

    @Override
    protected AbstractOTLPTransportAction createAction() {
        ClusterService clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ProjectMetadata projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().projectMetadata(Map.of(ProjectId.DEFAULT, projectMetadata)).build())
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        metricsAction = new OTLPMetricsTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            client,
            clusterService
        );
        return metricsAction;
    }

    @Override
    protected OTLPActionRequest createRequestWithData() {
        return createMetricsRequest(createMetric());
    }

    @Override
    protected OTLPActionRequest createEmptyRequest() {
        return createMetricsRequest();
    }

    @Override
    protected boolean parseHasPartialSuccess(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).hasPartialSuccess();
    }

    @Override
    protected long parseRejectedCount(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).getPartialSuccess().getRejectedDataPoints();
    }

    @Override
    protected String parseErrorMessage(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).getPartialSuccess().getErrorMessage();
    }

    @Override
    protected String dataStreamType() {
        return "metrics";
    }

    // --- metrics-specific tests ---

    public void testMappingHintsSettingsUpdate() throws Exception {
        assertThat(metricsAction.defaultMappingHints, equalTo(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM));
        assertThat(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING.isDynamic(), equalTo(true));

        clusterSettings.applySettings(Settings.builder().put(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING.getKey(), "histogram").build());
        assertThat(metricsAction.defaultMappingHints, equalTo(MappingHints.DEFAULT_TDIGEST));

        clusterSettings.applySettings(
            Settings.builder().put(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING.getKey(), "exponential_histogram").build()
        );
        assertThat(metricsAction.defaultMappingHints, equalTo(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM));
    }

    // --- helpers ---

    private static OTLPActionRequest createMetricsRequest(Metric... metrics) {
        return new OTLPActionRequest(
            new BytesArray(
                ExportMetricsServiceRequest.newBuilder()
                    .addResourceMetrics(
                        OtlpUtils.createResourceMetrics(
                            List.of(keyValue("service.name", "test-service")),
                            List.of(OtlpUtils.createScopeMetrics("test", "1.0.0", List.of(metrics)))
                        )
                    )
                    .build()
                    .toByteArray()
            )
        );
    }

    private static Metric createMetric() {
        return OtlpUtils.createGaugeMetric("test.metric", "", List.of(OtlpUtils.createDoubleDataPoint(0)));
    }
}
