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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransportGetTopNFunctionsAction extends HandledTransportAction<GetStackTracesRequest, GetTopNFunctionsResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetTopNFunctionsAction.class);
    private static final StackFrame EMPTY_STACKFRAME = new StackFrame("", "", 0, 0);

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
        client.execute(GetStackTracesAction.INSTANCE, request, ActionListener.wrap(searchResponse -> {
            StopWatch processingWatch = new StopWatch("Processing response");
            GetTopNFunctionsResponse topNFunctionsResponse = buildTopNFunctions(searchResponse);
            log.debug(() -> watch.report() + " " + processingWatch.report());
            listener.onResponse(topNFunctionsResponse);
        }, listener::onFailure));
    }

    static GetTopNFunctionsResponse buildTopNFunctions(GetStackTracesResponse response) {
        TopNFunctionsBuilder builder = new TopNFunctionsBuilder(response.getSamplingRate());
        if (response.getTotalFrames() == 0) {
            return builder.build();
        }

        for (Map.Entry<String, StackTrace> st : response.getStackTraces().entrySet()) {
            Set<String> frameGroupsPerStackTrace = new HashSet<>();
            String stackTraceId = st.getKey();
            StackTrace stackTrace = st.getValue();
            long samples = response.getStackTraceEvents().getOrDefault(stackTraceId, 0L);
            builder.addTotalCount(samples);

            int frameCount = stackTrace.frameIds.size();
            for (int i = 0; i < frameCount; i++) {
                String frameId = stackTrace.frameIds.get(i);
                String fileId = stackTrace.fileIds.get(i);
                Integer frameType = stackTrace.typeIds.get(i);
                Integer addressOrLine = stackTrace.addressOrLines.get(i);
                StackFrame stackFrame = response.getStackFrames().getOrDefault(frameId, EMPTY_STACKFRAME);
                String executable = response.getExecutables().getOrDefault(fileId, "");

                for (Frame frame : stackFrame.frames()) {
                    // The samples associated with a frame provide the total number of
                    // traces in which that frame has appeared at least once. However, a
                    // frame may appear multiple times in a trace, and thus to avoid
                    // counting it multiple times we need to record the frames seen so
                    // far in each trace. Instead of using the entire frame information
                    // to determine if a frame has already been seen within a given
                    // stacktrace, we use the frame group ID for a frame.
                    String frameGroupId = FrameGroupID.create(fileId, addressOrLine, executable, frame.fileName(), frame.functionName());
                    if (builder.setCurrentTopNFunction(frameGroupId) == false) {
                        builder.addTopNFunction(
                            frameGroupId,
                            new StackFrameMetadata(
                                frameId,
                                fileId,
                                frameType,
                                frame.inline(),
                                addressOrLine,
                                frame.functionName(),
                                frame.functionOffset(),
                                frame.fileName(),
                                frame.lineNumber(),
                                executable
                            )
                        );
                    }
                    if (frameGroupsPerStackTrace.contains(frameGroupId) == false) {
                        frameGroupsPerStackTrace.add(frameGroupId);
                        builder.addToCurrentInclusiveCount(samples);
                    }
                    if (i == frameCount - 1) {
                        // Leaf frame: sum up counts for exclusive CPU.
                        builder.addToCurrentExclusiveCount(samples);
                    }
                }
            }
        }

        return builder.build();
    }

    private static class TopNFunctionsBuilder {
        private long totalCount = 0;
        private TopNFunction currentTopNFunction;
        private final double samplingRate;
        private final HashMap<String, TopNFunction> topNFunctions;

        TopNFunctionsBuilder(double samplingRate) {
            this.samplingRate = samplingRate;
            this.topNFunctions = new HashMap<>();
        }

        public GetTopNFunctionsResponse build() {
            List<TopNFunction> functions = new ArrayList<>(topNFunctions.values());
            Collections.sort(functions, Collections.reverseOrder());
            long sumSelfCPU = 0;
            long sumTotalCPU = 0;
            for (int i = 0; i < functions.size(); i++) {
                TopNFunction topNFunction = functions.get(i);
                topNFunction.rank = i + 1;
                sumSelfCPU += topNFunction.exclusiveCount;
                sumTotalCPU += topNFunction.inclusiveCount;
            }
            return new GetTopNFunctionsResponse(samplingRate, totalCount, sumSelfCPU, sumTotalCPU, functions);
        }

        public void addTotalCount(long count) {
            this.totalCount += count;
        }

        public boolean setCurrentTopNFunction(String frameGroupID) {
            TopNFunction topNFunction = this.topNFunctions.get(frameGroupID);
            if (topNFunction == null) {
                return false;
            }
            this.currentTopNFunction = topNFunction;
            return true;
        }

        public void addTopNFunction(String frameGroupID, StackFrameMetadata metadata) {
            TopNFunction topNFunction = new TopNFunction(frameGroupID, metadata);
            this.currentTopNFunction = topNFunction;
            this.topNFunctions.put(frameGroupID, topNFunction);
        }

        public void addToCurrentExclusiveCount(long count) {
            this.currentTopNFunction.exclusiveCount += count;
        }

        public void addToCurrentInclusiveCount(long count) {
            this.currentTopNFunction.inclusiveCount += count;
        }
    }
}
