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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OTLPMetricsTransportActionTests extends AbstractOTLPTransportActionTests {

    private OTLPMetricsTransportAction metricsAction;
    private ClusterSettings clusterSettings;

    @Override
    protected AbstractOTLPTransportAction createAction() {
        metricsAction = createMetricsAction(Settings.EMPTY);
        return metricsAction;
    }

    private OTLPMetricsTransportAction createMetricsAction(Settings settings) {
        ClusterService clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ProjectMetadata projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().projectMetadata(Map.of(ProjectId.DEFAULT, projectMetadata)).build())
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        return new OTLPMetricsTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            client,
            clusterService,
            settings
        );
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

    public void testAttributeFanoutReturns413() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_CONTENT_LENGTH.getKey(), "1kb")
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_EXPANDED_CONTENT_LENGTH.getKey(), "10kb")
            .build();
        OTLPMetricsTransportAction action = createMetricsAction(settings);

        // Distinct data-point attributes force separate documents; a large resource attribute is copied into each.
        String largeValue = "x".repeat(1024);
        List<Metric> metrics = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            metrics.add(
                OtlpUtils.createGaugeMetric(
                    "test.metric",
                    "",
                    List.of(OtlpUtils.createDoubleDataPoint(i, i, List.of(keyValue("series", String.valueOf(i)))))
                )
            );
        }
        OTLPActionRequest request = new OTLPActionRequest(
            new BytesArray(
                OtlpUtils.createMetricsRequest(
                    List.of(keyValue("resource.large", largeValue), keyValue("service.name", "test-service")),
                    metrics
                ).toByteArray()
            )
        );

        @SuppressWarnings("unchecked")
        ActionListener<OTLPActionResponse> responseListener = mock(ActionListener.class);
        action.doExecute(null, request, responseListener);

        ArgumentCaptor<Exception> exception = ArgumentCaptor.forClass(Exception.class);
        verify(responseListener).onFailure(exception.capture());
        assertThat(ExceptionsHelper.status(exception.getValue()), equalTo(RestStatus.REQUEST_ENTITY_TOO_LARGE));
        assertThat(exception.getValue().getMessage(), containsString("expanded content would exceed limit"));
        verify(client, never()).execute(any(), any(), any());
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
