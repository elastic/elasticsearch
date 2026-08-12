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

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.MetricEscfConverter;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) Metrics requests.
 * This action processes the incoming metrics data, groups data points, and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the metrics.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * <p>When the batch-indexing feature is enabled and all target indices are resolvable TSDB write
 * indices, the action uses {@link MetricEscfConverter} to build ESCF batches directly from the
 * protobufs, bypassing the intermediate XContent (CBOR/JSON) round-trip. If any target index
 * cannot be resolved (non-existent data stream, non-TSDB index, unsupported version), the whole
 * request falls back to the per-row path via {@link MetricDocumentBuilder}.
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

    /**
     * Overrides the base class to attempt the fast ESCF (columnar) path when batch indexing is
     * enabled. Falls back to the standard row path via {@code super.doExecute} on any resolver
     * miss, gate failure, or unexpected exception from the converter.
     */
    @Override
    protected void doExecute(Task task, OTLPActionRequest request, ActionListener<OTLPActionResponse> listener) {
        if (batchIndexingGatesOpen()) {
            ExportMetricsServiceRequest parsed;
            try {
                parsed = ExportMetricsServiceRequest.parseFrom(request.getRequest().streamInput());
            } catch (InvalidProtocolBufferException e) {
                listener.onFailure(
                    new ElasticsearchStatusException("Invalid OTLP protobuf payload: " + e.getMessage(), RestStatus.BAD_REQUEST, e)
                );
                return;
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }

            MetricEscfConverter.Result escfResult = null;
            try {
                ProjectMetadata projectMetadata = clusterService.state().projectState(ProjectId.DEFAULT).metadata();
                MetricEscfConverter.TargetIndexResolver resolver = buildResolver(projectMetadata);
                escfResult = MetricEscfConverter.convert(parsed, defaultMappingHints, resolver);
                if (escfResult != null) {
                    // Transfer ownership to the releasing listener so it closes escfResult after
                    // the bulk response arrives (success or failure) or on an early listener call.
                    MetricEscfConverter.Result finalResult = escfResult;
                    escfResult = null;
                    ActionListener<OTLPActionResponse> releasingListener = ActionListener.releaseAfter(listener, finalResult);
                    try {
                        doExecuteEscf(parsed, finalResult, releasingListener);
                    } catch (Exception e) {
                        // doExecuteEscf threw before the listener was wired; onFailure closes finalResult.
                        releasingListener.onFailure(e);
                    }
                    return;
                }
                // resolver returned null — whole-request fallback to row path (fall through below)
            } catch (Exception e) {
                if (escfResult != null) {
                    escfResult.close();
                }
                // Unexpected converter error — propagate; the row path can't help here.
                listener.onFailure(e);
                return;
            }
        }
        // Row path: re-parse the protobuf inside prepareBulkRequest.
        super.doExecute(task, request, listener);
    }

    /**
     * Builds the bulk request from the pre-converted ESCF result and executes it. Each
     * {@link MetricEscfConverter.GroupResult} becomes one sourceless {@link IndexRequest} carrying
     * a row reference; the batches are attached so the coordinator can route them to shards.
     * The supplied listener is already wrapped with {@link ActionListener#releaseAfter} and will
     * close the result on completion.
     */
    private void doExecuteEscf(
        ExportMetricsServiceRequest parsed,
        MetricEscfConverter.Result result,
        ActionListener<OTLPActionResponse> listener
    ) throws IOException {
        if (result.groups().isEmpty()) {
            listener.onResponse(new OTLPActionResponse(BytesArray.EMPTY));
            return;
        }

        // Re-group from the already-parsed (in-memory) protobuf to get accurate totalItems() /
        // getIgnoredItems() for partial-success reporting. This traverses the object graph only —
        // no I/O, no row building.
        DataPointGroupingContext ctx = new DataPointGroupingContext(new BufferedByteStringAccessor(), defaultMappingHints);
        ctx.groupDataPoints(parsed);

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        Map<String, SourceBatch[]> batchesByIndexName = result.batchesByIndexName();

        for (MetricEscfConverter.GroupResult g : result.groups()) {
            IndexRequest ir = new IndexRequest(g.targetIndex()).opType(DocWriteRequest.OpType.CREATE)
                .setRequireDataStream(true)
                .setIncludeSourceOnError(false)
                .setDynamicTemplates(g.dynamicTemplates())
                .setDynamicTemplateParams(g.dynamicTemplateParams());
            ir.tsid(g.tsid());
            ir.setTimeSeriesTimestamp(Instant.EPOCH.plusNanos(g.timestampNanos()));
            // Source is carried in the ESCF batch; the row index drives shard-side reconstruction.
            // XContentType.CBOR matches the fallback content type used by the row path (ensureInlineSource).
            ir.indexSource().setSourceRow(batchesByIndexName.get(g.targetIndex())[g.shardId()], g.rowIndex(), XContentType.CBOR);
            bulkRequestBuilder.add(ir);
        }

        bulkRequestBuilder.setPreBuiltBatches(batchesByIndexName);

        ProcessingContext finalCtx = ctx;
        bulkRequestBuilder.execute(listener.delegateFailure((delegate, bulkResponse) -> {
            if (bulkResponse.hasFailures() || finalCtx.getIgnoredItems() > 0) {
                handlePartialSuccess(bulkResponse, finalCtx, delegate);
            } else {
                delegate.onResponse(new OTLPActionResponse(BytesArray.EMPTY));
            }
        }));
    }

    /**
     * Returns {@code true} when all three batch-indexing gates are satisfied:
     * the cluster setting is on, the feature flag is enabled, and the cluster's minimum
     * transport version supports {@link BulkShardRequest#BULK_SHARD_BATCH}.
     */
    private boolean batchIndexingGatesOpen() {
        return ShardBatchIndexer.BATCH_INDEXING.get(clusterService.getSettings())
            && ShardBatchIndexer.BATCH_INDEXING_FEATURE_FLAG.isEnabled()
            && clusterService.state().getMinTransportVersion().supports(BulkShardRequest.BULK_SHARD_BATCH);
    }

    /**
     * Builds a {@link MetricEscfConverter.TargetIndexResolver} that looks up the TSDB write index
     * for each data stream name and returns the shard-routing facts needed by the converter.
     * Results are memoized per call to avoid redundant cluster-state lookups within a request.
     * Returns {@code null} for any index that is not a resolvable TSDB write index.
     */
    private MetricEscfConverter.TargetIndexResolver buildResolver(ProjectMetadata projectMetadata) {
        Map<String, MetricEscfConverter.TargetIndexResolver.Target> resolvedTargets = new HashMap<>();
        Set<String> unresolvedTargets = new HashSet<>();
        return indexName -> {
            if (unresolvedTargets.contains(indexName)) {
                return null;
            }
            if (resolvedTargets.containsKey(indexName)) {
                return resolvedTargets.get(indexName);
            }
            MetricEscfConverter.TargetIndexResolver.Target target = resolveTarget(projectMetadata, indexName);
            if (target == null) {
                unresolvedTargets.add(indexName);
            } else {
                resolvedTargets.put(indexName, target);
            }
            return target;
        };
    }

    /**
     * Resolves one index name to a {@link MetricEscfConverter.TargetIndexResolver.Target}.
     * Returns {@code null} if the data stream does not exist, has no write index, uses a pre-TSDB
     * index version, or is not routed via {@link IndexRouting.ExtractFromSource.ForIndexDimensions}.
     */
    private static MetricEscfConverter.TargetIndexResolver.Target resolveTarget(ProjectMetadata projectMetadata, String indexName) {
        DataStream dataStream = projectMetadata.dataStreams().get(indexName);
        if (dataStream == null) {
            DataStreamAlias alias = projectMetadata.dataStreamAliases().get(indexName);
            if (alias != null && alias.getWriteDataStream() != null) {
                dataStream = projectMetadata.dataStreams().get(alias.getWriteDataStream());
            }
        }
        if (dataStream == null || dataStream.getWriteIndex() == null) {
            return null; // non-existent or newly-created data stream — no write index yet
        }
        IndexMetadata indexMetadata = projectMetadata.getIndexSafe(dataStream.getWriteIndex());
        IndexVersion indexVersion = indexMetadata.getCreationVersion();
        if (indexVersion.before(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG)) {
            return null; // old index version — TSID layout mismatch risk
        }
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
        if (!(indexRouting instanceof IndexRouting.ExtractFromSource.ForIndexDimensions dims)) {
            return null; // not a TSDB (dimensions-based) index
        }
        return new MetricEscfConverter.TargetIndexResolver.Target(indexVersion, indexMetadata.getNumberOfShards(), dims::shardIdForTsid);
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
