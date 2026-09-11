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
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricColumnarBuilder;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) Metrics requests.
 * This action processes the incoming metrics data, groups data points, and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the metrics.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * <p>When {@link BatchIndexingEnabled} is active cluster-wide <em>and</em> all data-point values and
 * attributes in the request are scalar
 * (gauges and monotonic sums with string/bool/int/double attributes only), metrics are written
 * directly into an {@link EscfBatch} without a CBOR intermediate representation, and the coordinator
 * derives {@code _tsid} column-major via
 * {@link org.elasticsearch.cluster.routing.ColumnarTsidCalculator}. For all other cases
 * (histograms, summaries, non-scalar attributes, or the setting disabled) the existing XContent path
 * is used unchanged.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPMetricsTransportAction extends AbstractOTLPTransportAction {

    public static final String NAME = "indices:data/write/otlp/metrics";
    public static final ActionType<OTLPActionResponse> TYPE = new ActionType<>(NAME);

    // visible for testing
    volatile MappingHints defaultMappingHints;
    private final ClusterService clusterService;
    private final BatchIndexingEnabled batchIndexingEnabled;

    @Inject
    public OTLPMetricsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService,
        Settings settings
    ) {
        super(NAME, transportService, actionFilters, threadPool, client, settings);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        defaultMappingHints = MappingHints.fromSettings(clusterSettings.get(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING));
        clusterSettings.addSettingsUpdateConsumer(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING, histogramFieldTypeSetting -> {
            defaultMappingHints = MappingHints.fromSettings(histogramFieldTypeSetting);
        });
        this.clusterService = clusterService;
        this.batchIndexingEnabled = new BatchIndexingEnabled(clusterSettings);
    }

    @Override
    protected ExportMetricsServiceResponse responseWithRejectedItems(int rejectedItems, String message) {
        ExportMetricsPartialSuccess partialSuccess = ExportMetricsPartialSuccess.newBuilder()
            .setRejectedDataPoints(rejectedItems)
            .setErrorMessage(message)
            .build();
        return ExportMetricsServiceResponse.newBuilder().setPartialSuccess(partialSuccess).build();
    }

    @Override
    protected ProcessingContext prepareBulkRequest(OTLPActionRequest request, BulkRequestBuilder bulkRequestBuilder) throws IOException {
        BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
        DataPointGroupingContext context = new DataPointGroupingContext(byteStringAccessor, defaultMappingHints);
        var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.getRequest().streamInput());
        context.groupDataPoints(metricsServiceRequest);
        if (context.totalItems() == 0) {
            return context;
        }

        ProjectMetadata projectMetadata = clusterService.state().projectState(ProjectId.DEFAULT).metadata();

        // Collect all groups in a single pass so we can check ESCF eligibility before committing any rows.
        // A BulkRequest carries at most one pre-built batch (keyed by index-abstraction name), so the ESCF
        // path requires all groups to target the same data stream. Groups targeting different data streams
        // fall through to doc-mode. Within a single data stream, BatchModeRouter handles routing to
        // multiple backing indices automatically via timestamp-based resolution.
        List<DataPointGroupingContext.DataPointGroup> allGroups = new ArrayList<>();
        context.consume(allGroups::add);

        String firstTarget = allGroups.isEmpty() ? null : allGroups.get(0).targetIndex().index();
        boolean singleTarget = firstTarget != null && allGroups.stream().allMatch(g -> firstTarget.equals(g.targetIndex().index()));

        if (singleTarget && resolveEscfEligible(projectMetadata, firstTarget, allGroups) && isEscfEligible(allGroups)) {
            MetricColumnarBuilder metricColumnarBuilder = new MetricColumnarBuilder(defaultMappingHints);
            addEscfBatch(bulkRequestBuilder, metricColumnarBuilder, allGroups, firstTarget);
            return context;
        }

        long totalExpandedBytes = 0;
        for (DataPointGroupingContext.DataPointGroup group : allGroups) {
            IndexVersion indexVersion = resolveIndexVersion(projectMetadata, group);
            MetricDocumentBuilder metricDocumentBuilder = new MetricDocumentBuilder(byteStringAccessor, defaultMappingHints);
            totalExpandedBytes = addIndexRequestDocMode(bulkRequestBuilder, metricDocumentBuilder, group, indexVersion, totalExpandedBytes);
        }

        return context;
    }

    // -------------------------------------------------------------------------
    // ESCF path
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} when every group in the list can be written as scalar ESCF columns, i.e. all
     * data points support columnar values and all attributes are scalar (no ARRAY/KVLIST/BYTES values).
     */
    static boolean isEscfEligible(List<DataPointGroupingContext.DataPointGroup> groups) {
        for (DataPointGroupingContext.DataPointGroup group : groups) {
            for (int i = 0; i < group.dataPoints().size(); i++) {
                if (group.dataPoints().get(i).supportsColumnarValue() == false) {
                    return false;
                }
            }
            if (MetricColumnarBuilder.hasNonScalarAttributes(group.dataPointAttributes())) {
                return false;
            }
            if (MetricColumnarBuilder.hasNonScalarAttributes(group.resource().getAttributesList())) {
                return false;
            }
            if (MetricColumnarBuilder.hasNonScalarAttributes(group.scope().getAttributesList())) {
                return false;
            }
        }
        return true;
    }

    private void addEscfBatch(
        BulkRequestBuilder bulkRequestBuilder,
        MetricColumnarBuilder metricColumnarBuilder,
        List<DataPointGroupingContext.DataPointGroup> groups,
        String target
    ) throws IOException {
        // Collect (rowIndex -> IndexRequest) during the build pass, then attach source rows after buildPartition.
        Map<Integer, IndexRequest> rowRequests = new LinkedHashMap<>(groups.size());
        try (EscfBatchBuilder batchBuilder = new EscfBatchBuilder()) {
            for (DataPointGroupingContext.DataPointGroup group : groups) {
                var dynamicTemplates = Maps.<String, String>newHashMapWithExpectedSize(group.dataPoints().size());
                var dynamicTemplateParams = Maps.<String, Map<String, String>>newHashMapWithExpectedSize(group.dataPoints().size());

                boolean ok = metricColumnarBuilder.buildMetricRow(batchBuilder, group, dynamicTemplates, dynamicTemplateParams);
                if (ok == false) {
                    // Eligibility pre-check should have prevented this; fail loudly.
                    throw new IllegalStateException(
                        "ESCF pre-flight check passed but buildMetricRow returned false for target [" + target + "]"
                    );
                }
                int rowIndex = batchBuilder.commit(0);

                Instant tsTimestamp = DataStream.getCanonicalTimestampBound(
                    Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(group.getTimestampUnixNano()))
                );
                IndexRequest indexRequest = new IndexRequest(target).opType(DocWriteRequest.OpType.CREATE)
                    .setRequireDataStream(true)
                    .setIncludeSourceOnError(false)
                    .setDynamicTemplates(dynamicTemplates)
                    .setDynamicTemplateParams(dynamicTemplateParams)
                    .setTimeSeriesTimestamp(tsTimestamp);
                // Source row will be attached after buildPartition below.
                rowRequests.put(rowIndex, indexRequest);
                bulkRequestBuilder.add(indexRequest);
            }

            // Partition 0 is the only partition: EscfBatchBuilder supports multiple keyed partitions
            // for producers that pre-split rows by shard, but we keep all rows in one flat batch and
            // let BatchModeRouter / EscfBatchScatterer handle shard scatter downstream.
            EscfBatch batch = batchBuilder.buildPartition(0);

            // Attach source rows now that the batch object is stable.
            for (Map.Entry<Integer, IndexRequest> e : rowRequests.entrySet()) {
                e.getValue().indexSource().setSourceRow(batch, e.getKey(), XContentType.JSON);
            }

            // Register the pre-built batch. BatchModeRouter will scatter it to shards and invoke
            // ForIndexDimensions.indexShard(requests, batch), which calls ColumnarTsidCalculator to
            // derive _tsid column-major (no pre-set tsid required on the IndexRequests).
            bulkRequestBuilder.setPreBuiltBatches(Map.of(target, batch));
            // Note: the EscfBatch (and its backing columns) is closed when the try-with-resources block
            // for EscfBatchBuilder exits. The batch data has been serialised at routing time, so the
            // backing columns are no longer needed after the bulk completes.
        }
    }

    // -------------------------------------------------------------------------
    // Doc-mode (XContent) path — unchanged from original implementation
    // -------------------------------------------------------------------------

    private long addIndexRequestDocMode(
        BulkRequestBuilder bulkRequestBuilder,
        MetricDocumentBuilder metricDocumentBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        IndexVersion indexVersion,
        long totalExpandedBytes
    ) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
            var dynamicTemplates = Maps.<String, String>newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            var dynamicTemplateParams = Maps.<String, Map<String, String>>newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            String dataStreamName = dataPointGroup.targetIndex().index();
            BytesRef tsid = metricDocumentBuilder.buildMetricDocument(
                xContentBuilder,
                dataPointGroup,
                dynamicTemplates,
                dynamicTemplateParams,
                indexVersion
            );
            var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                .setRequireDataStream(true)
                .source(xContentBuilder)
                .setIncludeSourceOnError(false)
                .setDynamicTemplates(dynamicTemplates)
                .setDynamicTemplateParams(dynamicTemplateParams);
            if (indexVersion.onOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG)) {
                indexRequest.tsid(tsid);
            }
            totalExpandedBytes = accountExpandedContent(totalExpandedBytes, indexRequest);
            bulkRequestBuilder.add(indexRequest);
        }
        return totalExpandedBytes;
    }

    private IndexVersion resolveIndexVersion(ProjectMetadata projectMetadata, DataPointGroupingContext.DataPointGroup group) {
        String dataStreamName = group.targetIndex().index();
        DataStream dataStream = projectMetadata.dataStreams().get(dataStreamName);
        if (dataStream == null) {
            DataStreamAlias alias = projectMetadata.dataStreamAliases().get(dataStreamName);
            if (alias != null && alias.getWriteDataStream() != null) {
                dataStream = projectMetadata.dataStreams().get(alias.getWriteDataStream());
            }
        }
        if (dataStream == null) {
            return IndexVersion.current();
        }
        Instant ts = Instant.ofEpochMilli(group.getTimestampUnixNano() / 1_000_000L);
        Index index = dataStream.selectTimeSeriesWriteIndex(ts, projectMetadata);
        if (index == null) {
            index = dataStream.getWriteIndex();
        }
        return projectMetadata.getIndexSafe(index).getCreationVersion();
    }

    private boolean resolveEscfEligible(
        ProjectMetadata projectMetadata,
        String dataStreamName,
        List<DataPointGroupingContext.DataPointGroup> allGroups
    ) {
        if (batchIndexingEnabled.isEnabled() == false) {
            return false;
        }
        DataStream dataStream = projectMetadata.dataStreams().get(dataStreamName);
        if (dataStream == null) {
            DataStreamAlias alias = projectMetadata.dataStreamAliases().get(dataStreamName);
            if (alias != null && alias.getWriteDataStream() != null) {
                dataStream = projectMetadata.dataStreams().get(alias.getWriteDataStream());
            }
        }
        // Data stream not yet initialised → fall back to doc-mode so the first export creates it.
        if (dataStream == null) {
            return false;
        }
        // Determine which backing indices the batch actually targets based on document timestamps,
        // then verify each one supports ESCF (has dimensions and a recent enough creation version).
        long[] timestamps = new long[allGroups.size()];
        for (int i = 0; i < allGroups.size(); i++) {
            timestamps[i] = allGroups.get(i).getTimestampUnixNano();
        }
        for (Index index : dataStream.selectTimeSeriesWriteIndices(timestamps, projectMetadata)) {
            IndexMetadata im = projectMetadata.getIndexSafe(index);
            if (im.getTimeSeriesDimensions().isEmpty() || IndexSettings.TIME_SERIES_BATCH_INDEXING.get(im.getSettings()) == false) {
                return false;
            }
        }
        return true;
    }
}
