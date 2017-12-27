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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MLMetadataField;
import org.elasticsearch.xpack.ml.MlClientHelper;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TransportPreviewDatafeedAction extends HandledTransportAction<PreviewDatafeedAction.Request, PreviewDatafeedAction.Response> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportPreviewDatafeedAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                          Client client, ClusterService clusterService) {
        super(settings, PreviewDatafeedAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                PreviewDatafeedAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(PreviewDatafeedAction.Request request, ActionListener<PreviewDatafeedAction.Response> listener) {
        MlMetadata mlMetadata = clusterService.state().getMetaData().custom(MLMetadataField.TYPE);
        DatafeedConfig datafeed = mlMetadata.getDatafeed(request.getDatafeedId());
        if (datafeed == null) {
            throw ExceptionsHelper.missingDatafeedException(request.getDatafeedId());
        }
        Job job = mlMetadata.getJobs().get(datafeed.getJobId());
        if (job == null) {
            throw ExceptionsHelper.missingJobException(datafeed.getJobId());
        }
        DatafeedConfig.Builder datafeedWithAutoChunking = new DatafeedConfig.Builder(datafeed);
        datafeedWithAutoChunking.setChunkingConfig(ChunkingConfig.newAuto());
        Map<String, String> headers = threadPool.getThreadContext().getHeaders().entrySet().stream()
                .filter(e -> MlClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        datafeedWithAutoChunking.setHeaders(headers);
        // NB: this is using the client from the transport layer, NOT the internal client.
        // This is important because it means the datafeed search will fail if the user
        // requesting the preview doesn't have permission to search the relevant indices.
        DataExtractorFactory.create(client, datafeedWithAutoChunking.build(), job, new ActionListener<DataExtractorFactory>() {
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
