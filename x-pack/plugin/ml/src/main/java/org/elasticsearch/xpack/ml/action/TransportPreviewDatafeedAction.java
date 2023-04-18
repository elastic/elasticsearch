/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.cluster.routing.allocation.mapper.DataTierFieldMapper;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeWithHeadersAsync;
import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.ml.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

public class TransportPreviewDatafeedAction extends HandledTransportAction<PreviewDatafeedAction.Request, PreviewDatafeedAction.Response> {

    private final ThreadPool threadPool;
    private final Client client;
    private final ClusterService clusterService;
    private final JobConfigProvider jobConfigProvider;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final NamedXContentRegistry xContentRegistry;
    private final SecurityContext securityContext;

    @Inject
    public TransportPreviewDatafeedAction(
        Settings settings,
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        JobConfigProvider jobConfigProvider,
        DatafeedConfigProvider datafeedConfigProvider,
        NamedXContentRegistry xContentRegistry
    ) {
        super(PreviewDatafeedAction.NAME, transportService, actionFilters, PreviewDatafeedAction.Request::new);
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService = clusterService;
        this.jobConfigProvider = jobConfigProvider;
        this.datafeedConfigProvider = datafeedConfigProvider;
        this.xContentRegistry = xContentRegistry;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
    }

    @Override
    protected void doExecute(Task task, PreviewDatafeedAction.Request request, ActionListener<PreviewDatafeedAction.Response> listener) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        ActionListener<DatafeedConfig> datafeedConfigActionListener = ActionListener.wrap(datafeedConfig -> {
            if (request.getJobConfig() != null) {
                previewDatafeed(parentTaskId, datafeedConfig, request.getJobConfig().build(new Date()), request, listener);
                return;
            }
            jobConfigProvider.getJob(
                datafeedConfig.getJobId(),
                parentTaskId,
                ActionListener.wrap(
                    jobBuilder -> previewDatafeed(parentTaskId, datafeedConfig, jobBuilder.build(), request, listener),
                    listener::onFailure
                )
            );
        }, listener::onFailure);
        if (request.getDatafeedConfig() != null) {
            datafeedConfigActionListener.onResponse(request.getDatafeedConfig());
        } else {
            datafeedConfigProvider.getDatafeedConfig(
                request.getDatafeedId(),
                parentTaskId,
                ActionListener.wrap(builder -> datafeedConfigActionListener.onResponse(builder.build()), listener::onFailure)
            );
        }
    }

    private void previewDatafeed(
        TaskId parentTaskId,
        DatafeedConfig datafeedConfig,
        Job job,
        PreviewDatafeedAction.Request request,
        ActionListener<PreviewDatafeedAction.Response> listener
    ) {
        DatafeedConfig.Builder previewDatafeedBuilder = buildPreviewDatafeed(datafeedConfig);
        useSecondaryAuthIfAvailable(securityContext, () -> {
            previewDatafeedBuilder.setHeaders(
                ClientHelper.getPersistableSafeSecurityHeaders(threadPool.getThreadContext(), clusterService.state())
            );
            // NB: this is using the client from the transport layer, NOT the internal client.
            // This is important because it means the datafeed search will fail if the user
            // requesting the preview doesn't have permission to search the relevant indices.
            DatafeedConfig previewDatafeedConfig = previewDatafeedBuilder.build();
            DataExtractorFactory.create(
                new ParentTaskAssigningClient(client, parentTaskId),
                previewDatafeedConfig,
                job,
                xContentRegistry,
                // Fake DatafeedTimingStatsReporter that does not have access to results index
                new DatafeedTimingStatsReporter(new DatafeedTimingStats(datafeedConfig.getJobId()), (ts, refreshPolicy, listener1) -> {}),
                listener.delegateFailure(
                    (l, dataExtractorFactory) -> isDateNanos(
                        previewDatafeedConfig,
                        job.getDataDescription().getTimeField(),
                        listener.delegateFailure((l2, isDateNanos) -> {
                            final QueryBuilder hotOnly = QueryBuilders.boolQuery()
                                .mustNot(QueryBuilders.termsQuery(DataTierFieldMapper.NAME, "data_frozen", "data_cold"));
                            final long start = request.getStartTime().orElse(0);
                            final long end = request.getEndTime()
                                .orElse(isDateNanos ? DateUtils.MAX_NANOSECOND_INSTANT.toEpochMilli() : Long.MAX_VALUE);
                            DataExtractor dataExtractor = request.getStartTime().isPresent() || request.getEndTime().isPresent()
                                ? dataExtractorFactory.newExtractor(start, end)
                                : dataExtractorFactory.newExtractor(start, end, hotOnly);
                            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> previewDatafeed(dataExtractor, l));
                        })
                    )
                )
            );
        });
    }

    /** Visible for testing */
    static DatafeedConfig.Builder buildPreviewDatafeed(DatafeedConfig datafeed) {

        // Since we only want a preview, it's worth limiting the cost
        // of the search in the case of non-aggregated datafeeds.
        // We do so by setting auto-chunking. This ensures to find
        // a sensible time range with enough data to preview.
        // When aggregations are present, it's best to comply with
        // what the datafeed is set to do as it can reveal problems with
        // the datafeed config (e.g. a chunking config that would hit circuit-breakers).
        DatafeedConfig.Builder previewDatafeed = new DatafeedConfig.Builder(datafeed);
        if (datafeed.hasAggregations() == false) {
            previewDatafeed.setChunkingConfig(ChunkingConfig.newAuto());
        }
        return previewDatafeed;
    }

    private void isDateNanos(DatafeedConfig datafeed, String timeField, ActionListener<Boolean> listener) {
        FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest();
        fieldCapabilitiesRequest.indices(datafeed.getIndices().toArray(new String[0])).indicesOptions(datafeed.getIndicesOptions());
        fieldCapabilitiesRequest.fields(timeField);
        executeWithHeadersAsync(
            datafeed.getHeaders(),
            ML_ORIGIN,
            client,
            FieldCapabilitiesAction.INSTANCE,
            fieldCapabilitiesRequest,
            ActionListener.wrap(fieldCapsResponse -> {
                Map<String, FieldCapabilities> timeFieldCaps = fieldCapsResponse.getField(timeField);
                listener.onResponse(timeFieldCaps.containsKey(DateFieldMapper.DATE_NANOS_CONTENT_TYPE));
            }, listener::onFailure)
        );
    }

    /** Visible for testing */
    static void previewDatafeed(DataExtractor dataExtractor, ActionListener<PreviewDatafeedAction.Response> listener) {
        try {
            Optional<InputStream> inputStream = dataExtractor.next().data();
            // DataExtractor returns single-line JSON but without newline characters between objects.
            // Instead, it has a space between objects due to how JSON XContentBuilder works.
            // In order to return a proper JSON array from preview, we surround with square brackets and
            // we stick in a comma between objects.
            // Also, the stream is expected to be a single line but in case it is not, we join lines
            // using space to ensure the comma insertion works correctly.
            StringBuilder responseBuilder = new StringBuilder("[");
            if (inputStream.isPresent()) {
                try (BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream.get(), StandardCharsets.UTF_8))) {
                    responseBuilder.append(buffer.lines().collect(Collectors.joining(" ")).replace("} {", "},{"));
                }
            }
            responseBuilder.append("]");
            listener.onResponse(
                new PreviewDatafeedAction.Response(new BytesArray(responseBuilder.toString().getBytes(StandardCharsets.UTF_8)))
            );
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            dataExtractor.cancel();
        }
    }
}
