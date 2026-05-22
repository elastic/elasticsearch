/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) Metrics requests.
 * This action processes the incoming metrics data, groups data points, and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the metrics.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPMetricsTransportAction extends AbstractOTLPTransportAction {

    public static final String NAME = "indices:data/write/otlp/metrics";
    public static final ActionType<OTLPActionResponse> TYPE = new ActionType<>(NAME);

    // visible for testing
    volatile MappingHints defaultMappingHints;
    private final ClusterService clusterService;

    @Inject
    public OTLPMetricsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService
    ) {
        super(NAME, transportService, actionFilters, threadPool, client);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        defaultMappingHints = MappingHints.fromSettings(clusterSettings.get(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING));
        clusterSettings.addSettingsUpdateConsumer(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING, histogramFieldTypeSetting -> {
            defaultMappingHints = MappingHints.fromSettings(histogramFieldTypeSetting);
        });
        this.clusterService = clusterService;
    }

    @Override
    protected ProcessingContext prepareBulkRequest(OTLPActionRequest request, BulkRequestBuilder bulkRequestBuilder) throws IOException {
        BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
        DataPointGroupingContext context = new DataPointGroupingContext(byteStringAccessor);
        var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.getRequest().streamInput());
        context.groupDataPoints(metricsServiceRequest);
        if (context.totalItems() == 0) {
            return context;
        }
        MetricDocumentBuilder metricDocumentBuilder = new MetricDocumentBuilder(byteStringAccessor, defaultMappingHints);
        ProjectMetadata projectMetadata = clusterService.state().projectState(ProjectId.DEFAULT).metadata();
        Map<String, IndexVersion> indexVersions = new HashMap<>();
        context.consume(
            dataPointGroup -> addIndexRequest(bulkRequestBuilder, metricDocumentBuilder, dataPointGroup, projectMetadata, indexVersions)
        );
        return context;
    }

    @Override
    protected ExportMetricsServiceResponse responseWithRejectedItems(int rejectedItems, String message) {
        ExportMetricsPartialSuccess partialSuccess = ExportMetricsPartialSuccess.newBuilder()
            .setRejectedDataPoints(rejectedItems)
            .setErrorMessage(message)
            .build();
        return ExportMetricsServiceResponse.newBuilder().setPartialSuccess(partialSuccess).build();
    }

    private static IndexVersion resolveIndexVersion(ProjectMetadata projectMetadata, String dataStreamName) {
        DataStream dataStream = projectMetadata.dataStreams().get(dataStreamName);
        if (dataStream == null) {
            DataStreamAlias alias = projectMetadata.dataStreamAliases().get(dataStreamName);
            if (alias != null && alias.getWriteDataStream() != null) {
                dataStream = projectMetadata.dataStreams().get(alias.getWriteDataStream());
            }
        }
        if (dataStream != null && dataStream.getWriteIndex() != null) {
            return projectMetadata.getIndexSafe(dataStream.getWriteIndex()).getCreationVersion();
        }
        // non-existent data-stream will be created with the current index version
        return IndexVersion.current();
    }

    private void addIndexRequest(
        BulkRequestBuilder bulkRequestBuilder,
        MetricDocumentBuilder metricDocumentBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        ProjectMetadata projectMetadata,
        Map<String, IndexVersion> indexVersions
    ) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
            var dynamicTemplates = Maps.<String, String>newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            var dynamicTemplateParams = Maps.<String, Map<String, String>>newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            String dataStreamName = dataPointGroup.targetIndex().index();
            IndexVersion indexVersion = indexVersions.computeIfAbsent(dataStreamName, name -> resolveIndexVersion(projectMetadata, name));
            BytesRef tsid = metricDocumentBuilder.buildMetricDocument(
                xContentBuilder,
                dataPointGroup,
                dynamicTemplates,
                dynamicTemplateParams,
                indexVersion
            );
            var indexRequest = new IndexRequest(dataPointGroup.targetIndex().index()).opType(DocWriteRequest.OpType.CREATE)
                .setRequireDataStream(true)
                .source(xContentBuilder)
                .setIncludeSourceOnError(false)
                .setDynamicTemplates(dynamicTemplates)
                .setDynamicTemplateParams(dynamicTemplateParams);
            // For old write indices, let the indexing layer compute the TSID — avoids layout mismatch if a rollover occurs mid-request.
            if (indexVersion.onOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG)) {
                indexRequest.tsid(tsid);
            }
            bulkRequestBuilder.add(indexRequest);
        }
    }
}
