/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportGetTopNFunctionsAction extends HandledTransportAction<GetStackTracesRequest, GetTopNFunctionsResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetTopNFunctionsAction.class);

    private final NodeClient nodeClient;
    private final TransportService transportService;

    @Inject
    public TransportGetTopNFunctionsAction(NodeClient nodeClient, TransportService transportService, ActionFilters actionFilters) {
        super(
            GetTopNFunctionsAction.NAME,
            transportService,
            actionFilters,
            GetStackTracesRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.nodeClient = nodeClient;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, GetStackTracesRequest request, ActionListener<GetTopNFunctionsResponse> listener) {
        Client client = new ParentTaskAssigningClient(this.nodeClient, transportService.getLocalNode(), task);
        StopWatch watch = new StopWatch("getTopNFunctionsAction");
        client.execute(GetStackTracesAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(GetStackTracesResponse response) {
                long responseStart = System.nanoTime();
                try {
                    StopWatch processingWatch = new StopWatch("Processing response");
                    GetTopNFunctionsResponse topNFunctionsResponse = buildTopNFunctions(response);
                    log.debug(() -> watch.report() + " " + processingWatch.report());
                    listener.onResponse(topNFunctionsResponse);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    static GetTopNFunctionsResponse buildTopNFunctions(GetStackTracesResponse response) {
        TopNFunctionsBuilder builder = new TopNFunctionsBuilder(0, response.getSamplingRate());
        if (response.getTotalFrames() == 0) {
            return builder.build();
        }

        return builder.build();
    }

    private static class TopNFunctionsBuilder {
        private int size = 0;
        private final double samplingRate;

        TopNFunctionsBuilder(int frames, double samplingRate) {
            this.samplingRate = samplingRate;
        }

        public GetTopNFunctionsResponse build() {
            return new GetTopNFunctionsResponse(size, samplingRate);
        }
    }
}
