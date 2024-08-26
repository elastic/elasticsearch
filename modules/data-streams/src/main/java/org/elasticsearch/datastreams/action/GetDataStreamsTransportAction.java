/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.DataStreamsActionUtil;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction.Response.IndexProperties;
import org.elasticsearch.action.datastreams.GetDataStreamAction.Response.ManagedBy;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.IndexSettings.PREFER_ILM_SETTING;

public class GetDataStreamsTransportAction extends TransportMasterNodeReadAction<
    GetDataStreamAction.Request,
    GetDataStreamAction.Response> {

    private static final Logger LOGGER = LogManager.getLogger(GetDataStreamsTransportAction.class);
    private final SystemIndices systemIndices;
    private final ClusterSettings clusterSettings;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;

    @Inject
    public GetDataStreamsTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        super(
            GetDataStreamAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDataStreamAction.Request::new,
            indexNameExpressionResolver,
            GetDataStreamAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.systemIndices = systemIndices;
        this.globalRetentionSettings = globalRetentionSettings;
        clusterSettings = clusterService.getClusterSettings();
    }

    @Override
    protected void masterOperation(
        Task task,
        GetDataStreamAction.Request request,
        ClusterState state,
        ActionListener<GetDataStreamAction.Response> listener
    ) throws Exception {
        listener.onResponse(
            innerOperation(state, request, indexNameExpressionResolver, systemIndices, clusterSettings, globalRetentionSettings)
        );
    }

    static GetDataStreamAction.Response innerOperation(
        ClusterState state,
        GetDataStreamAction.Request request,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices,
        ClusterSettings clusterSettings,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        List<DataStream> dataStreams = getDataStreams(state, indexNameExpressionResolver, request);
        List<GetDataStreamAction.Response.DataStreamInfo> dataStreamInfos = new ArrayList<>(dataStreams.size());
        for (DataStream dataStream : dataStreams) {
            final String indexTemplate;
            boolean indexTemplatePreferIlmValue = true;
            String ilmPolicyName = null;
            if (dataStream.isSystem()) {
                SystemDataStreamDescriptor dataStreamDescriptor = systemIndices.findMatchingDataStreamDescriptor(dataStream.getName());
                indexTemplate = dataStreamDescriptor != null ? dataStreamDescriptor.getDataStreamName() : null;
                if (dataStreamDescriptor != null) {
                    Settings settings = MetadataIndexTemplateService.resolveSettings(
                        dataStreamDescriptor.getComposableIndexTemplate(),
                        dataStreamDescriptor.getComponentTemplates()
                    );
                    ilmPolicyName = settings.get(IndexMetadata.LIFECYCLE_NAME);
                    indexTemplatePreferIlmValue = PREFER_ILM_SETTING.get(settings);
                }
            } else {
                indexTemplate = MetadataIndexTemplateService.findV2Template(state.metadata(), dataStream.getName(), false);
                if (indexTemplate != null) {
                    Settings settings = MetadataIndexTemplateService.resolveSettings(state.metadata(), indexTemplate);
                    ilmPolicyName = settings.get(IndexMetadata.LIFECYCLE_NAME);
                    indexTemplatePreferIlmValue = PREFER_ILM_SETTING.get(settings);
                } else {
                    LOGGER.warn(
                        "couldn't find any matching template for data stream [{}]. has it been restored (and possibly renamed)"
                            + "from a snapshot?",
                        dataStream.getName()
                    );
                }
            }

            ClusterStateHealth streamHealth = new ClusterStateHealth(
                state,
                dataStream.getIndices().stream().map(Index::getName).toArray(String[]::new)
            );

            Map<Index, IndexProperties> backingIndicesSettingsValues = new HashMap<>();
            Metadata metadata = state.getMetadata();
            collectIndexSettingsValues(dataStream, backingIndicesSettingsValues, metadata, dataStream.getIndices());
            if (DataStream.isFailureStoreFeatureFlagEnabled() && dataStream.getFailureIndices().getIndices().isEmpty() == false) {
                collectIndexSettingsValues(dataStream, backingIndicesSettingsValues, metadata, dataStream.getFailureIndices().getIndices());
            }

            GetDataStreamAction.Response.TimeSeries timeSeries = null;
            if (dataStream.getIndexMode() == IndexMode.TIME_SERIES) {
                record IndexInfo(String name, Instant timeSeriesStart, Instant timeSeriesEnd) implements Comparable<IndexInfo> {
                    @Override
                    public int compareTo(IndexInfo o) {
                        return Comparator.comparing(IndexInfo::timeSeriesStart).thenComparing(IndexInfo::timeSeriesEnd).compare(this, o);
                    }
                }

                List<Tuple<Instant, Instant>> mergedRanges = new ArrayList<>();
                Tuple<Instant, Instant> currentMergedRange = null;
                IndexInfo previous = null;

                // We need indices to be sorted by time series range
                // to produce temporal ranges.
                // But it is not enforced in API, so we explicitly sort here.
                var sortedRanges = dataStream.getIndices()
                    .stream()
                    .map(metadata::index)
                    .filter(m -> m.getIndexMode() == IndexMode.TIME_SERIES)
                    .map(m -> new IndexInfo(m.getIndex().getName(), m.getTimeSeriesStart(), m.getTimeSeriesEnd()))
                    .sorted()
                    .toList();

                for (var info : sortedRanges) {
                    Instant start = info.timeSeriesStart();
                    Instant end = info.timeSeriesEnd();

                    if (currentMergedRange == null) {
                        currentMergedRange = new Tuple<>(start, end);
                    } else if (currentMergedRange.v2().compareTo(start) == 0) {
                        currentMergedRange = new Tuple<>(currentMergedRange.v1(), end);
                    } else if (currentMergedRange.v2().compareTo(start) < 0) {
                        mergedRanges.add(currentMergedRange);
                        currentMergedRange = new Tuple<>(start, end);
                    } else {
                        String message = "previous backing index ["
                            + previous.name()
                            + "] range ["
                            + previous.timeSeriesStart()
                            + "/"
                            + previous.timeSeriesEnd()
                            + "] range is colliding with current backing ["
                            + info.name()
                            + "] index range ["
                            + start
                            + "/"
                            + end
                            + "]";
                        assert currentMergedRange.v2().compareTo(start) < 0 : message;
                    }
                    previous = info;
                }
                if (currentMergedRange != null) {
                    mergedRanges.add(currentMergedRange);
                }
                timeSeries = new GetDataStreamAction.Response.TimeSeries(mergedRanges);
            }

            dataStreamInfos.add(
                new GetDataStreamAction.Response.DataStreamInfo(
                    dataStream,
                    streamHealth.getStatus(),
                    indexTemplate,
                    ilmPolicyName,
                    timeSeries,
                    backingIndicesSettingsValues,
                    indexTemplatePreferIlmValue
                )
            );
        }
        return new GetDataStreamAction.Response(
            dataStreamInfos,
            request.includeDefaults() ? clusterSettings.get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING) : null,
            globalRetentionSettings.get()
        );
    }

    private static void collectIndexSettingsValues(
        DataStream dataStream,
        Map<Index, IndexProperties> backingIndicesSettingsValues,
        Metadata metadata,
        List<Index> backingIndices
    ) {
        for (Index index : backingIndices) {
            IndexMetadata indexMetadata = metadata.index(index);
            Boolean preferIlm = PREFER_ILM_SETTING.get(indexMetadata.getSettings());
            assert preferIlm != null : "must use the default prefer ilm setting value, if nothing else";
            ManagedBy managedBy;
            if (metadata.isIndexManagedByILM(indexMetadata)) {
                managedBy = ManagedBy.ILM;
            } else if (dataStream.isIndexManagedByDataStreamLifecycle(index, metadata::index)) {
                managedBy = ManagedBy.LIFECYCLE;
            } else {
                managedBy = ManagedBy.UNMANAGED;
            }
            backingIndicesSettingsValues.put(index, new IndexProperties(preferIlm, indexMetadata.getLifecyclePolicyName(), managedBy));
        }
    }

    static List<DataStream> getDataStreams(
        ClusterState clusterState,
        IndexNameExpressionResolver iner,
        GetDataStreamAction.Request request
    ) {
        List<String> results = DataStreamsActionUtil.getDataStreamNames(iner, clusterState, request.getNames(), request.indicesOptions());
        Map<String, DataStream> dataStreams = clusterState.metadata().dataStreams();

        return results.stream().map(dataStreams::get).sorted(Comparator.comparing(DataStream::getName)).toList();
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
