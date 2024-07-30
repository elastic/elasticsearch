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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class TransportGetFlamegraphAction extends TransportAction<GetStackTracesRequest, GetFlamegraphResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetFlamegraphAction.class);
    private final NodeClient nodeClient;
    private final TransportService transportService;

    @Inject
    public TransportGetFlamegraphAction(NodeClient nodeClient, TransportService transportService, ActionFilters actionFilters) {
        super(GetFlamegraphAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
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
            StackTrace stackTrace = st.getValue();
            builder.setCurrentNode(0);

            long samples = stackTrace.count;
            builder.addSamplesInclusive(0, samples);
            builder.addSamplesExclusive(0, 0L);

            double annualCO2Tons = stackTrace.annualCO2Tons;
            builder.addAnnualCO2TonsInclusive(0, annualCO2Tons);
            builder.addAnnualCO2TonsExclusive(0, 0.0d);

            double annualCostsUSD = stackTrace.annualCostsUSD;
            builder.addAnnualCostsUSDInclusive(0, annualCostsUSD);
            builder.addAnnualCostsUSDExclusive(0, 0.0d);

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
                    String frameGroupId = FrameGroupID.create(fileId, addressOrLine, executable, frame.fileName(), frame.functionName());

                    int nodeId;
                    if (builder.isExists(frameGroupId)) {
                        nodeId = builder.getNodeId(frameGroupId);
                        builder.addSamplesInclusive(nodeId, samples);
                        builder.addAnnualCO2TonsInclusive(nodeId, annualCO2Tons);
                        builder.addAnnualCostsUSDInclusive(nodeId, annualCostsUSD);
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
                            annualCO2Tons,
                            annualCostsUSD,
                            frameGroupId
                        );
                    }
                    if (isLeafFrame && frame.last()) {
                        // Leaf frame: sum up counts for exclusive CPU.
                        builder.addSamplesExclusive(nodeId, samples);
                        builder.addAnnualCO2TonsExclusive(nodeId, annualCO2Tons);
                        builder.addAnnualCostsUSDExclusive(nodeId, annualCostsUSD);
                    }
                    builder.setCurrentNode(nodeId);
                });
            }
        }
        return builder.build();
    }

    private static class FlamegraphBuilder {
        private int currentNode = 0;
        // size is the number of nodes in the flamegraph
        private int size = 0;
        private long selfCPU;
        private long totalCPU;
        // totalSamples is the total number of samples in the stacktraces
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
        private final List<Long> countInclusive;
        private final List<Long> countExclusive;
        private final List<Double> annualCO2TonsExclusive;
        private final List<Double> annualCO2TonsInclusive;
        private final List<Double> annualCostsUSDExclusive;
        private final List<Double> annualCostsUSDInclusive;
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
            this.annualCO2TonsInclusive = new ArrayList<>(capacity);
            this.annualCO2TonsExclusive = new ArrayList<>(capacity);
            this.annualCostsUSDInclusive = new ArrayList<>(capacity);
            this.annualCostsUSDExclusive = new ArrayList<>(capacity);
            this.totalSamples = totalSamples;
            // always insert root node
            int nodeId = this.addNode("", 0, false, "", 0, "", 0, "", 0, 0, 0.0, 0.0, null);
            this.setCurrentNode(nodeId);
            this.samplingRate = samplingRate;
        }

        // returns the new node's id
        public int addNode(
            String fileId,
            int frameType,
            boolean inline,
            String fileName,
            int addressOrLine,
            String functionName,
            int functionOffset,
            String sourceFileName,
            int sourceLine,
            long samples,
            double annualCO2Tons,
            double annualCostsUSD,
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
            this.countExclusive.add(0L);
            this.annualCO2TonsInclusive.add(annualCO2Tons);
            this.annualCO2TonsExclusive.add(0.0);
            this.annualCostsUSDInclusive.add(annualCostsUSD);
            this.annualCostsUSDExclusive.add(0.0);
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

        public void addSamplesInclusive(int nodeId, long sampleCount) {
            Long priorSampleCount = this.countInclusive.get(nodeId);
            this.countInclusive.set(nodeId, priorSampleCount + sampleCount);
            this.totalCPU += sampleCount;
        }

        public void addSamplesExclusive(int nodeId, long sampleCount) {
            Long priorSampleCount = this.countExclusive.get(nodeId);
            this.countExclusive.set(nodeId, priorSampleCount + sampleCount);
            this.selfCPU += sampleCount;
        }

        public void addAnnualCO2TonsInclusive(int nodeId, double annualCO2Tons) {
            Double priorAnnualCO2Tons = this.annualCO2TonsInclusive.get(nodeId);
            this.annualCO2TonsInclusive.set(nodeId, priorAnnualCO2Tons + annualCO2Tons);
        }

        public void addAnnualCO2TonsExclusive(int nodeId, double annualCO2Tons) {
            Double priorAnnualCO2Tons = this.annualCO2TonsExclusive.get(nodeId);
            this.annualCO2TonsExclusive.set(nodeId, priorAnnualCO2Tons + annualCO2Tons);
        }

        public void addAnnualCostsUSDInclusive(int nodeId, double annualCostsUSD) {
            Double priorAnnualCostsUSD = this.annualCostsUSDInclusive.get(nodeId);
            this.annualCostsUSDInclusive.set(nodeId, priorAnnualCostsUSD + annualCostsUSD);
        }

        public void addAnnualCostsUSDExclusive(int nodeId, double annualCostsUSD) {
            Double priorAnnualCostsUSD = this.annualCostsUSDExclusive.get(nodeId);
            this.annualCostsUSDExclusive.set(nodeId, priorAnnualCostsUSD + annualCostsUSD);
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
                annualCO2TonsInclusive,
                annualCO2TonsExclusive,
                annualCostsUSDInclusive,
                annualCostsUSDExclusive,
                selfCPU,
                totalCPU,
                totalSamples
            );
        }
    }
}
