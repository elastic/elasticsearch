/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransportGetTopNFunctionsAction extends TransportAction<GetStackTracesRequest, GetTopNFunctionsResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetTopNFunctionsAction.class);
    private final NodeClient nodeClient;
    private final TransportService transportService;

    @Inject
    public TransportGetTopNFunctionsAction(NodeClient nodeClient, TransportService transportService, ActionFilters actionFilters) {
        super(GetTopNFunctionsAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.nodeClient = nodeClient;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, GetStackTracesRequest request, ActionListener<GetTopNFunctionsResponse> listener) {
        Client client = new ParentTaskAssigningClient(this.nodeClient, transportService.getLocalNode(), task);
        StopWatch watch = new StopWatch("getTopNFunctionsAction");
        client.execute(GetStackTracesAction.INSTANCE, request, ActionListener.wrap(searchResponse -> {
            StopWatch processingWatch = new StopWatch("Processing response");
            GetTopNFunctionsResponse topNFunctionsResponse = buildTopNFunctions(searchResponse, request.getLimit());
            log.debug(() -> watch.report() + " " + processingWatch.report());
            listener.onResponse(topNFunctionsResponse);
        }, listener::onFailure));
    }

    static GetTopNFunctionsResponse buildTopNFunctions(GetStackTracesResponse response, Integer limit) {
        TopNFunctionsBuilder builder = new TopNFunctionsBuilder(limit);
        if (response.getTotalFrames() == 0) {
            return builder.build();
        }

        for (StackTrace stackTrace : response.getStackTraces().values()) {
            Set<String> frameGroupsPerStackTrace = new HashSet<>();
            long samples = stackTrace.count;
            double annualCO2Tons = stackTrace.annualCO2Tons;
            double annualCostsUSD = stackTrace.annualCostsUSD;

            int frameCount = stackTrace.frameIds.length;
            for (int i = 0; i < frameCount; i++) {
                String frameId = stackTrace.frameIds[i];
                String fileId = stackTrace.fileIds[i];
                int frameType = stackTrace.typeIds[i];
                int addressOrLine = stackTrace.addressOrLines[i];
                StackFrame stackFrame = response.getStackFrames().getOrDefault(frameId, StackFrame.EMPTY_STACKFRAME);
                String executable = response.getExecutables().getOrDefault(fileId, "");

                final boolean isLeafFrame = i == frameCount - 1;
                stackFrame.forEach(frame -> {
                    // The samples associated with a frame provide the total number of
                    // traces in which that frame has appeared at least once. However, a
                    // frame may appear multiple times in a trace, and thus to avoid
                    // counting it multiple times we need to record the frames seen so
                    // far in each trace. Instead of using the entire frame information
                    // to determine if a frame has already been seen within a given
                    // stacktrace, we use the frame group ID for a frame.
                    String frameGroupId = FrameGroupID.create(fileId, addressOrLine, executable, frame.fileName(), frame.functionName());
                    if (builder.isExists(frameGroupId) == false) {
                        builder.addTopNFunction(
                            new TopNFunction(
                                frameGroupId,
                                frameType,
                                frame.inline(),
                                addressOrLine,
                                frame.functionName(),
                                frame.fileName(),
                                frame.lineNumber(),
                                executable
                            )
                        );
                    }
                    TopNFunction current = builder.getTopNFunction(frameGroupId);
                    if (stackTrace.subGroups != null) {
                        current.addSubGroups(stackTrace.subGroups);
                    }
                    if (frameGroupsPerStackTrace.contains(frameGroupId) == false) {
                        frameGroupsPerStackTrace.add(frameGroupId);
                        current.addTotalCount(samples);
                        current.addTotalAnnualCO2Tons(annualCO2Tons);
                        current.addTotalAnnualCostsUSD(annualCostsUSD);

                    }
                    if (isLeafFrame && frame.last()) {
                        // Leaf frame: sum up counts for self CPU.
                        current.addSelfCount(samples);
                        current.addSelfAnnualCO2Tons(annualCO2Tons);
                        current.addSelfAnnualCostsUSD(annualCostsUSD);

                    }
                });
            }
        }

        return builder.build();
    }

    static class TopNFunctionsBuilder {
        private final Integer limit;
        private final HashMap<String, TopNFunction> topNFunctions;

        TopNFunctionsBuilder(Integer limit) {
            this.limit = limit;
            this.topNFunctions = new HashMap<>();
        }

        public GetTopNFunctionsResponse build() {
            List<TopNFunction> functions = new ArrayList<>(topNFunctions.values());
            functions.sort(Collections.reverseOrder());
            long sumSelfCount = 0;
            long sumTotalCount = 0;
            double sumAnnualCo2Tons = 0.0d;
            double sumAnnualCostsUsd = 0.0d;

            for (int i = 0; i < functions.size(); i++) {
                TopNFunction topNFunction = functions.get(i);
                topNFunction.setRank(i + 1);
                sumSelfCount += topNFunction.getSelfCount();
                sumTotalCount += topNFunction.getTotalCount();
                sumAnnualCo2Tons += topNFunction.getSelfAnnualCO2Tons();
                sumAnnualCostsUsd += topNFunction.getSelfAnnualCostsUSD();
            }
            // limit at the end so global stats are independent of the limit
            if (limit != null && limit > 0 && limit < functions.size()) {
                functions = functions.subList(0, limit);
            }
            return new GetTopNFunctionsResponse(sumSelfCount, sumTotalCount, sumAnnualCo2Tons, sumAnnualCostsUsd, functions);
        }

        public boolean isExists(String frameGroupID) {
            return this.topNFunctions.containsKey(frameGroupID);
        }

        public TopNFunction getTopNFunction(String frameGroupID) {
            return this.topNFunctions.get(frameGroupID);
        }

        public void addTopNFunction(TopNFunction topNFunction) {
            this.topNFunctions.put(topNFunction.getId(), topNFunction);
        }
    }
}
