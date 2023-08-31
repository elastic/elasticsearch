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
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
        super(GetFlamegraphAction.NAME, transportService, actionFilters, GetStackTracesRequest::new);
        this.nodeClient = nodeClient;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, GetStackTracesRequest request, ActionListener<GetFlamegraphResponse> listener) {
        Client client = new ParentTaskAssigningClient(this.nodeClient, transportService.getLocalNode(), task);
        long start = System.nanoTime();
        client.execute(GetStackTracesAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(GetStackTracesResponse response) {
                long responseStart = System.nanoTime();
                try {
                    GetFlamegraphResponse flamegraphResponse = buildFlamegraph(response);
                    log.debug(
                        "getFlamegraphAction took ["
                            + (System.nanoTime() - start) / 1_000_000.0d
                            + "] ms (processing response: ["
                            + (System.nanoTime() - responseStart) / 1_000_000.0d
                            + "] ms."
                    );
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

    private GetFlamegraphResponse buildFlamegraph(GetStackTracesResponse response) {
        // TODO: Mind that the Kibana implementation calls "decodeStackTraceResponse" immediately to flatten inline stackframes!
        // TODO: The number of total frames does *not* include inlined frames!
        // TODO: Determine total seconds!!!
        FlamegraphBuilder builder = new FlamegraphBuilder(response.getTotalFrames(), response.getSamplingRate(), 0.0d);
        if (response.getTotalFrames() == 0) {
            return builder.build();
        }

        SortedMap<String, StackTrace> sortedStacktraces = new TreeMap<>(response.getStackTraces());
        // The inverse of the sampling rate is the number with which to multiply the number of
        // samples to get an estimate of the actual number of samples the backend received.
        double scalingFactor = 1.0d / response.getSamplingRate();

        for (Map.Entry<String, StackTrace> st : sortedStacktraces.entrySet()) {
            String stackTraceId = st.getKey();
            StackTrace stackTrace = st.getValue();
            int samples = (int) Math.floor(response.getStackTraceEvents().getOrDefault(stackTraceId, 0) * scalingFactor);

            int frameCount = stackTrace.frameIds.size();
            for (int i = 0; i < frameCount; i++) {
                String frameId = stackTrace.frameIds.get(i);
                String fileId = stackTrace.fileIds.get(i);
                Integer frameType = stackTrace.typeIds.get(i);
                Integer addressOrLine = stackTrace.addressOrLines.get(i);
                StackFrame stackFrame = response.getStackFrames().getOrDefault(frameId, EMPTY_STACKFRAME);
                String executable = response.getExecutables().getOrDefault(fileId, "");

                String frameGroupId = createFrameGroupID(
                    fileId,
                    addressOrLine,
                    executable,
                    // TODO: Iterate over (inlined) frames
                    stackFrame.fileName.isEmpty() ? "" : stackFrame.fileName.get(0),
                    stackFrame.functionName.isEmpty() ? "" : stackFrame.functionName.get(0)
                );

                int nodeId;
                if (builder.isExists(frameGroupId)) {
                    nodeId = builder.getNodeId(frameGroupId);
                    builder.addSamplesInclusive(nodeId, samples);
                } else {
                    // TODO: stackFrame.fileName needs to be only the file name (without the path) -> check whether this is really
                    // true
                    nodeId = builder.addNode(
                        fileId,
                        frameType,
                        // TODO: Determine inline flag properly
                        // inline,
                        false,
                        executable,
                        addressOrLine,
                        // TODO: Consider all frames, not only the first one!!!
                        stackFrame.functionName.isEmpty() ? "" : stackFrame.functionName.get(0),
                        stackFrame.functionOffset.isEmpty() ? 0 : stackFrame.functionOffset.get(0),
                        stackFrame.fileName.isEmpty() ? "" : stackFrame.fileName.get(0),
                        stackFrame.lineNumber.isEmpty() ? 0 : stackFrame.lineNumber.get(0),
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
        // TODO: Also consider createBaseFlameGraph() - this should be our final data structure
        return builder.build();
    }

    @SuppressForbidden(reason = "Using pathSeparator constant to extract the filename with low overhead")
    private static String getFilename(String fullPath) {
        if (fullPath == null) {
            return null;
        }
        int lastSeparatorIdx = fullPath.lastIndexOf(File.pathSeparator);
        return lastSeparatorIdx == -1 ? fullPath : fullPath.substring(lastSeparatorIdx + 1);
    }

    private static String createFrameGroupID(
        String fileID,
        Integer addressOrLine,
        String exeFilename,
        String sourceFilename,
        String functionName
    ) {
        if (functionName.isEmpty()) {
            return String.format(Locale.ROOT, "empty;%s;%d", fileID, addressOrLine);
        }

        if (sourceFilename.isEmpty()) {
            return String.format(Locale.ROOT, "elf;%s;%s", exeFilename, functionName);
        }
        // TODO: This should actually be stripLeadingSubdirs(sourceFilename || "")
        return String.format(Locale.ROOT, "full;%s;%s;%s", exeFilename, functionName, getFilename(sourceFilename));
    }

    private static class FlamegraphBuilder {
        private int currentNode = 0;
        private int size = 0;
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
        private final double totalSeconds;

        FlamegraphBuilder(int frames, double samplingRate, double totalSeconds) {
            this.edges = new ArrayList<>(frames);
            this.fileIds = new ArrayList<>(frames);
            this.frameTypes = new ArrayList<>(frames);
            this.inlineFrames = new ArrayList<>(frames);
            this.fileNames = new ArrayList<>(frames);
            this.addressOrLines = new ArrayList<>(frames);
            this.functionNames = new ArrayList<>(frames);
            this.functionOffsets = new ArrayList<>(frames);
            this.sourceFileNames = new ArrayList<>(frames);
            this.sourceLines = new ArrayList<>(frames);
            this.countInclusive = new ArrayList<>(frames);
            this.countExclusive = new ArrayList<>(frames);
            if (frames > 0) {
                // root node
                int nodeId = this.addNode("", 0, false, "", 0, "", 0, "", 0, 0, null);
                this.setCurrentNode(nodeId);
            }
            this.samplingRate = samplingRate;
            this.totalSeconds = totalSeconds;
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
        }

        public void addSamplesExclusive(int nodeId, int sampleCount) {
            Integer priorSampleCount = this.countExclusive.get(nodeId);
            this.countExclusive.set(nodeId, priorSampleCount + sampleCount);
        }

        public GetFlamegraphResponse build() {
            return new GetFlamegraphResponse(
                size,
                samplingRate,
                totalSeconds,
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
                countExclusive
            );
        }
    }
}
