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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class TransportGetFlamegraphAction extends HandledTransportAction<GetStackTracesRequest, GetFlamegraphResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetFlamegraphAction.class);
    private static final StackFrame EMPTY_STACKFRAME = new StackFrame("", "", 0, 0);

    private final NodeClient nodeClient;
    private final TransportService transportService;

    @Inject
    public TransportGetFlamegraphAction(NodeClient nodeClient, TransportService transportService, ActionFilters actionFilters) {
        super(GetFlamegraphAction.NAME, transportService, actionFilters, GetStackTracesRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.nodeClient = nodeClient;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, GetStackTracesRequest request, ActionListener<GetFlamegraphResponse> listener) {
        Client client = new ParentTaskAssigningClient(this.nodeClient, transportService.getLocalNode(), task);
        StopWatch watch = new StopWatch("getFlamegraphAction");
        client.execute(GetStackTracesAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(GetStackTracesResponse response) {
                try {
                    StopWatch processingWatch = new StopWatch("Processing response");
                    GetFlamegraphResponse flamegraphResponse = buildFlamegraph(response);
                    log.debug(() -> watch.report() + " " + processingWatch.report());
                    listener.onResponse(flamegraphResponse);
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

    static GetFlamegraphResponse buildFlamegraph(GetStackTracesResponse response) {
        FlamegraphBuilder builder = new FlamegraphBuilder(
            response.getTotalSamples(),
            response.getTotalFrames(),
            response.getSamplingRate()
        );
        if (response.getTotalFrames() == 0) {
            return builder.build();
        }

        SortedMap<String, StackTrace> sortedStacktraces = new TreeMap<>(response.getStackTraces());
        for (Map.Entry<String, StackTrace> st : sortedStacktraces.entrySet()) {
            String stackTraceId = st.getKey();
            StackTrace stackTrace = st.getValue();
            int samples = response.getStackTraceEvents().getOrDefault(stackTraceId, 0);
            builder.setCurrentNode(0);
            builder.addSamplesInclusive(0, samples);
            builder.addSamplesExclusive(0, 0);

            int frameCount = stackTrace.frameIds.size();
            for (int i = 0; i < frameCount; i++) {
                String frameId = stackTrace.frameIds.get(i);
                String fileId = stackTrace.fileIds.get(i);
                Integer frameType = stackTrace.typeIds.get(i);
                Integer addressOrLine = stackTrace.addressOrLines.get(i);
                StackFrame stackFrame = response.getStackFrames().getOrDefault(frameId, EMPTY_STACKFRAME);
                String executable = response.getExecutables().getOrDefault(fileId, "");

                for (Frame frame : stackFrame.frames()) {
                    String frameGroupId = FrameGroupID.create(fileId, addressOrLine, executable, frame.fileName(), frame.functionName());

                    int nodeId;
                    if (builder.isExists(frameGroupId)) {
                        nodeId = builder.getNodeId(frameGroupId);
                        builder.addSamplesInclusive(nodeId, samples);
                    } else {
                        nodeId = builder.addNode(
                            fileId,
                            frameType,
                            frame.inline(),
                            executable,
                            addressOrLine,
                            frame.functionName(),
                            frame.functionOffset(),
                            frame.fileName(),
                            frame.lineNumber(),
                            samples,
                            frameGroupId
                        );
                    }
                    if (i == frameCount - 1) {
                        // Leaf frame: sum up counts for exclusive CPU.
                        builder.addSamplesExclusive(nodeId, samples);
                    }
                    builder.setCurrentNode(nodeId);
                }
            }
        }
        return builder.build();
    }

    private static class FlamegraphBuilder {
        private int currentNode = 0;
        private int size = 0;
        private int selfCPU;
        private int totalCPU;
        private final long totalSamples;
        // Map: FrameGroupId -> NodeId
        private final List<Map<String, Integer>> edges;
        private final List<String> fileIds;
        private final List<Integer> frameTypes;
        private final List<Boolean> inlineFrames;
        private final List<String> fileNames;
        private final List<Integer> addressOrLines;
        private final List<String> functionNames;
        private final List<Integer> functionOffsets;
        private final List<String> sourceFileNames;
        private final List<Integer> sourceLines;
        private final List<Integer> countInclusive;
        private final List<Integer> countExclusive;
        private final double samplingRate;

        FlamegraphBuilder(long totalSamples, int frames, double samplingRate) {
            // as the number of frames does not account for inline frames we slightly overprovision.
            int capacity = (int) (frames * 1.1d);
            this.edges = new ArrayList<>(capacity);
            this.fileIds = new ArrayList<>(capacity);
            this.frameTypes = new ArrayList<>(capacity);
            this.inlineFrames = new ArrayList<>(capacity);
            this.fileNames = new ArrayList<>(capacity);
            this.addressOrLines = new ArrayList<>(capacity);
            this.functionNames = new ArrayList<>(capacity);
            this.functionOffsets = new ArrayList<>(capacity);
            this.sourceFileNames = new ArrayList<>(capacity);
            this.sourceLines = new ArrayList<>(capacity);
            this.countInclusive = new ArrayList<>(capacity);
            this.countExclusive = new ArrayList<>(capacity);
            this.totalSamples = totalSamples;
            // always insert root node
            int nodeId = this.addNode("", 0, false, "", 0, "", 0, "", 0, 0, null);
            this.setCurrentNode(nodeId);
            this.samplingRate = samplingRate;
        }

        // returns the new node's id
        public int addNode(
            String fileId,
            int frameType,
            boolean inline,
            String fileName,
            Integer addressOrLine,
            String functionName,
            int functionOffset,
            String sourceFileName,
            int sourceLine,
            int samples,
            String frameGroupId
        ) {
            int node = this.size;
            this.edges.add(new HashMap<>());
            this.fileIds.add(fileId);
            this.frameTypes.add(frameType);
            this.inlineFrames.add(inline);
            this.fileNames.add(fileName);
            this.addressOrLines.add(addressOrLine);
            this.functionNames.add(functionName);
            this.functionOffsets.add(functionOffset);
            this.sourceFileNames.add(sourceFileName);
            this.sourceLines.add(sourceLine);
            this.countInclusive.add(samples);
            this.totalCPU += samples;
            this.countExclusive.add(0);
            if (frameGroupId != null) {
                this.edges.get(currentNode).put(frameGroupId, node);
            }
            this.size++;
            return node;
        }

        public void setCurrentNode(int nodeId) {
            this.currentNode = nodeId;
        }

        public boolean isExists(String frameGroupId) {
            return this.edges.get(currentNode).containsKey(frameGroupId);
        }

        public int getNodeId(String frameGroupId) {
            return this.edges.get(currentNode).get(frameGroupId);
        }

        public void addSamplesInclusive(int nodeId, int sampleCount) {
            Integer priorSampleCount = this.countInclusive.get(nodeId);
            this.countInclusive.set(nodeId, priorSampleCount + sampleCount);
            this.totalCPU += sampleCount;
        }

        public void addSamplesExclusive(int nodeId, int sampleCount) {
            Integer priorSampleCount = this.countExclusive.get(nodeId);
            this.countExclusive.set(nodeId, priorSampleCount + sampleCount);
            this.selfCPU += sampleCount;
        }

        public GetFlamegraphResponse build() {
            return new GetFlamegraphResponse(
                size,
                samplingRate,
                edges,
                fileIds,
                frameTypes,
                inlineFrames,
                fileNames,
                addressOrLines,
                functionNames,
                functionOffsets,
                sourceFileNames,
                sourceLines,
                countInclusive,
                countExclusive,
                selfCPU,
                totalCPU,
                totalSamples
            );
        }
    }
}
