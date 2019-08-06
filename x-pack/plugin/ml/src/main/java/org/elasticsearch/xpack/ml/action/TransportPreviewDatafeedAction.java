/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TransportPreviewDatafeedAction extends HandledTransportAction<PreviewDatafeedAction.Request, PreviewDatafeedAction.Response> {

    private final ThreadPool threadPool;
    private final Client client;
    private final JobConfigProvider jobConfigProvider;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final JobResultsProvider jobResultsProvider;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportPreviewDatafeedAction(ThreadPool threadPool, TransportService transportService,
                                          ActionFilters actionFilters, Client client, JobConfigProvider jobConfigProvider,
                                          DatafeedConfigProvider datafeedConfigProvider, JobResultsProvider jobResultsProvider,
                                          NamedXContentRegistry xContentRegistry) {
        super(PreviewDatafeedAction.NAME, transportService, actionFilters, PreviewDatafeedAction.Request::new);
        this.threadPool = threadPool;
        this.client = client;
        this.jobConfigProvider = jobConfigProvider;
        this.datafeedConfigProvider = datafeedConfigProvider;
        this.jobResultsProvider = jobResultsProvider;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, PreviewDatafeedAction.Request request, ActionListener<PreviewDatafeedAction.Response> listener) {
        datafeedConfigProvider.getDatafeedConfig(request.getDatafeedId(), ActionListener.wrap(
            datafeedConfigBuilder -> {
                DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
                jobConfigProvider.getJob(datafeedConfig.getJobId(), ActionListener.wrap(
                    jobBuilder -> {
                        DatafeedConfig.Builder previewDatafeed = buildPreviewDatafeed(datafeedConfig);
                        Map<String, String> headers = threadPool.getThreadContext().getHeaders().entrySet().stream()
                            .filter(e -> ClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        previewDatafeed.setHeaders(headers);
                        jobResultsProvider.datafeedTimingStats(
                            jobBuilder.getId(),
                            timingStats -> {
                                // NB: this is using the client from the transport layer, NOT the internal client.
                                // This is important because it means the datafeed search will fail if the user
                                // requesting the preview doesn't have permission to search the relevant indices.
                                DataExtractorFactory.create(
                                    client,
                                    previewDatafeed.build(),
                                    jobBuilder.build(),
                                    xContentRegistry,
                                    // Fake DatafeedTimingStatsReporter that does not have access to results index
                                    new DatafeedTimingStatsReporter(timingStats, (ts, refreshPolicy) -> {}),
                                    new ActionListener<>() {
                                        @Override
                                        public void onResponse(DataExtractorFactory dataExtractorFactory) {
                                            DataExtractor dataExtractor = dataExtractorFactory.newExtractor(0, Long.MAX_VALUE);
                                            threadPool.generic().execute(() -> previewDatafeed(dataExtractor, listener));
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            listener.onFailure(e);
                                        }
                                    });
                            },
                            listener::onFailure);
                    },
                    listener::onFailure));
            },
            listener::onFailure));
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

    /** Visible for testing */
    static void previewDatafeed(DataExtractor dataExtractor, ActionListener<PreviewDatafeedAction.Response> listener) {
        try {
            Optional<InputStream> inputStream = dataExtractor.next();
            // DataExtractor returns single-line JSON but without newline characters between objects.
            // Instead, it has a space between objects due to how JSON XContenetBuilder works.
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
            listener.onResponse(new PreviewDatafeedAction.Response(
                    new BytesArray(responseBuilder.toString().getBytes(StandardCharsets.UTF_8))));
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            dataExtractor.cancel();
        }
    }
}
