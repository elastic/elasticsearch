/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.DataStreamsActionUtil;
import org.elasticsearch.action.datastreams.DataStreamsStatsAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction.Response.IndexProperties;
import org.elasticsearch.action.datastreams.GetDataStreamAction.Response.ManagedBy;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.PREFER_ILM_SETTING;

public class TransportGetDataStreamsAction extends TransportLocalProjectMetadataAction<
    GetDataStreamAction.Request,
    GetDataStreamAction.Response> {

    private static final Logger LOGGER = LogManager.getLogger(TransportGetDataStreamsAction.class);
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SystemIndices systemIndices;
    private final ClusterSettings clusterSettings;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;
    private final DataStreamFailureStoreSettings dataStreamFailureStoreSettings;
    private final IndexSettingProviders indexSettingProviders;
    private final Client client;

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetDataStreamsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices,
        DataStreamGlobalRetentionSettings globalRetentionSettings,
        DataStreamFailureStoreSettings dataStreamFailureStoreSettings,
        IndexSettingProviders indexSettingProviders,
        Client client
    ) {
        super(
            GetDataStreamAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.systemIndices = systemIndices;
        this.globalRetentionSettings = globalRetentionSettings;
        clusterSettings = clusterService.getClusterSettings();
        this.dataStreamFailureStoreSettings = dataStreamFailureStoreSettings;
        this.indexSettingProviders = indexSettingProviders;
        this.client = new OriginSettingClient(client, "stack");

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetDataStreamAction.Request::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetDataStreamAction.Request request,
        ProjectState state,
        ActionListener<GetDataStreamAction.Response> listener
    ) throws Exception {
        ((CancellableTask) task).ensureNotCancelled();
        if (request.verbose()) {
            DataStreamsStatsAction.Request req = new DataStreamsStatsAction.Request();
            req.indices(request.indices());
            client.execute(DataStreamsStatsAction.INSTANCE, req, new ActionListener<>() {
                @Override
                public void onResponse(DataStreamsStatsAction.Response response) {
                    ((CancellableTask) task).ensureNotCancelled();
                    final Map<String, Long> maxTimestamps = Arrays.stream(response.getDataStreams())
                        .collect(
                            Collectors.toMap(
                                DataStreamsStatsAction.DataStreamStats::getDataStream,
                                DataStreamsStatsAction.DataStreamStats::getMaximumTimestamp
                            )
                        );
                    listener.onResponse(
                        innerOperation(
                            state,
                            request,
                            indexNameExpressionResolver,
                            systemIndices,
                            clusterSettings,
                            globalRetentionSettings,
                            dataStreamFailureStoreSettings,
                            indexSettingProviders,
                            maxTimestamps
                        )
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } else {
            listener.onResponse(
                innerOperation(
                    state,
                    request,
                    indexNameExpressionResolver,
                    systemIndices,
                    clusterSettings,
                    globalRetentionSettings,
                    dataStreamFailureStoreSettings,
                    indexSettingProviders,
                    null
                )
            );
        }
    }

    /**
     * Resolves the index mode ("index.mode" setting) for the given data stream, from the template or additional setting providers
     */
    @Nullable
    static IndexMode resolveMode(
        ProjectState state,
        IndexSettingProviders indexSettingProviders,
        DataStream dataStream,
        Settings settings,
        ComposableIndexTemplate indexTemplate
    ) {
        IndexMode indexMode = state.metadata().retrieveIndexModeFromTemplate(indexTemplate);
        for (IndexSettingProvider provider : indexSettingProviders.getIndexSettingProviders()) {
            Settings addlSettinsg = provider.getAdditionalIndexSettings(
                MetadataIndexTemplateService.VALIDATE_INDEX_NAME,
                dataStream.getName(),
                indexMode,
                state.metadata(),
                Instant.now(),
                settings,
                List.of()
            );
            var rawMode = addlSettinsg.get(IndexSettings.MODE.getKey());
            if (rawMode != null) {
                indexMode = Enum.valueOf(IndexMode.class, rawMode.toUpperCase(Locale.ROOT));
            }
        }
        return indexMode;
    }

    static GetDataStreamAction.Response innerOperation(
        ProjectState state,
        GetDataStreamAction.Request request,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices,
        ClusterSettings clusterSettings,
        DataStreamGlobalRetentionSettings globalRetentionSettings,
        DataStreamFailureStoreSettings dataStreamFailureStoreSettings,
        IndexSettingProviders indexSettingProviders,
        @Nullable Map<String, Long> maxTimestamps
    ) {
        List<DataStream> dataStreams = getDataStreams(state.metadata(), indexNameExpressionResolver, request);
        List<GetDataStreamAction.Response.DataStreamInfo> dataStreamInfos = new ArrayList<>(dataStreams.size());
        for (DataStream dataStream : dataStreams) {
            // For this action, we are returning whether the failure store is effectively enabled, either in metadata or by cluster setting.
            // Users can use the get data stream options API to find out whether it is explicitly enabled in metadata.
            boolean failureStoreEffectivelyEnabled = dataStream.isFailureStoreEffectivelyEnabled(dataStreamFailureStoreSettings);
            final String indexTemplate;
            boolean indexTemplatePreferIlmValue = true;
            String ilmPolicyName = null;
            IndexMode indexMode = dataStream.getIndexMode();
            if (dataStream.isSystem()) {
                SystemDataStreamDescriptor dataStreamDescriptor = systemIndices.findMatchingDataStreamDescriptor(dataStream.getName());
                indexTemplate = dataStreamDescriptor != null ? dataStreamDescriptor.getDataStreamName() : null;
                if (dataStreamDescriptor != null) {
                    Settings settings = MetadataIndexTemplateService.resolveSettings(
                        dataStreamDescriptor.getComposableIndexTemplate(),
                        dataStreamDescriptor.getComponentTemplates()
                    );
                    ilmPolicyName = settings.get(IndexMetadata.LIFECYCLE_NAME);
                    if (indexMode == null) {
                        indexMode = resolveMode(
                            state,
                            indexSettingProviders,
                            dataStream,
                            settings,
                            dataStreamDescriptor.getComposableIndexTemplate()
                        );
                    }
                    indexTemplatePreferIlmValue = PREFER_ILM_SETTING.get(settings);
                }
            } else {
                indexTemplate = MetadataIndexTemplateService.findV2Template(state.metadata(), dataStream.getName(), false);
                if (indexTemplate != null) {
                    Settings settings = dataStream.getEffectiveSettings(state.metadata());
                    ilmPolicyName = settings.get(IndexMetadata.LIFECYCLE_NAME);
                    if (indexMode == null && state.metadata().templatesV2().get(indexTemplate) != null) {
                        indexMode = resolveMode(
                            state,
                            indexSettingProviders,
                            dataStream,
                            settings,
                            dataStream.getEffectiveIndexTemplate(state.metadata())
                        );
                    }
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
                state.cluster(),
                dataStream.getIndices().stream().map(Index::getName).toArray(String[]::new),
                state.projectId()
            );

            Map<Index, IndexProperties> backingIndicesSettingsValues = new HashMap<>();
            ProjectMetadata metadata = state.metadata();
            collectIndexSettingsValues(dataStream, backingIndicesSettingsValues, metadata, dataStream.getIndices());
            if (dataStream.getFailureIndices().isEmpty() == false) {
                collectIndexSettingsValues(dataStream, backingIndicesSettingsValues, metadata, dataStream.getFailureIndices());
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
                    failureStoreEffectivelyEnabled,
                    streamHealth.getStatus(),
                    indexTemplate,
                    ilmPolicyName,
                    timeSeries,
                    backingIndicesSettingsValues,
                    indexTemplatePreferIlmValue,
                    maxTimestamps == null ? null : maxTimestamps.get(dataStream.getName()),
                    // Default to standard mode if not specified; should we set this to "unset" or "unspecified" instead?
                    indexMode == null ? IndexMode.STANDARD.getName() : indexMode.getName()
                )
            );
        }
        return new GetDataStreamAction.Response(
            dataStreamInfos,
            request.includeDefaults() ? clusterSettings.get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING) : null,
            globalRetentionSettings.get(false),
            globalRetentionSettings.get(true)
        );
    }

    private static void collectIndexSettingsValues(
        DataStream dataStream,
        Map<Index, IndexProperties> backingIndicesSettingsValues,
        ProjectMetadata metadata,
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
            String indexMode = IndexSettings.MODE.get(indexMetadata.getSettings()).getName();
            backingIndicesSettingsValues.put(
                index,
                new IndexProperties(preferIlm, indexMetadata.getLifecyclePolicyName(), managedBy, indexMode)
            );
        }
    }

    static List<DataStream> getDataStreams(ProjectMetadata project, IndexNameExpressionResolver iner, GetDataStreamAction.Request request) {
        List<String> results = DataStreamsActionUtil.getDataStreamNames(iner, project, request.getNames(), request.indicesOptions());
        Map<String, DataStream> dataStreams = project.dataStreams();

        return results.stream().map(dataStreams::get).sorted(Comparator.comparing(DataStream::getName)).toList();
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
