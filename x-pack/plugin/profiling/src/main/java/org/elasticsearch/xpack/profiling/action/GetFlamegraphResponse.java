/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GetFlamegraphResponse extends ActionResponse implements ChunkedToXContentObject {
    private final int size;
    private final double samplingRate;
    private final long selfCPU;
    @UpdateForV9(owner = UpdateForV9.Owner.PROFILING) // remove this field - it is unused in Kibana
    private final long totalCPU;
    @UpdateForV9(owner = UpdateForV9.Owner.PROFILING) // remove this field - it is unused in Kibana
    private final long totalSamples;
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
    private final List<Double> annualCO2TonsInclusive;
    private final List<Double> annualCO2TonsExclusive;
    private final List<Double> annualCostsUSDInclusive;
    private final List<Double> annualCostsUSDExclusive;

    public GetFlamegraphResponse(
        int size,
        double samplingRate,
        List<Map<String, Integer>> edges,
        List<String> fileIds,
        List<Integer> frameTypes,
        List<Boolean> inlineFrames,
        List<String> fileNames,
        List<Integer> addressOrLines,
        List<String> functionNames,
        List<Integer> functionOffsets,
        List<String> sourceFileNames,
        List<Integer> sourceLines,
        List<Long> countInclusive,
        List<Long> countExclusive,
        List<Double> annualCO2TonsInclusive,
        List<Double> annualCO2TonsExclusive,
        List<Double> annualCostsUSDInclusive,
        List<Double> annualCostsUSDExclusive,
        long selfCPU,
        long totalCPU,
        long totalSamples
    ) {
        this.size = size;
        this.samplingRate = samplingRate;
        this.edges = edges;
        this.fileIds = fileIds;
        this.frameTypes = frameTypes;
        this.inlineFrames = inlineFrames;
        this.fileNames = fileNames;
        this.addressOrLines = addressOrLines;
        this.functionNames = functionNames;
        this.functionOffsets = functionOffsets;
        this.sourceFileNames = sourceFileNames;
        this.sourceLines = sourceLines;
        this.countInclusive = countInclusive;
        this.countExclusive = countExclusive;
        this.annualCO2TonsInclusive = annualCO2TonsInclusive;
        this.annualCO2TonsExclusive = annualCO2TonsExclusive;
        this.annualCostsUSDInclusive = annualCostsUSDInclusive;
        this.annualCostsUSDExclusive = annualCostsUSDExclusive;
        this.selfCPU = selfCPU;
        this.totalCPU = totalCPU;
        this.totalSamples = totalSamples;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    public int getSize() {
        return size;
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    public List<Long> getCountInclusive() {
        return countInclusive;
    }

    public List<Long> getCountExclusive() {
        return countExclusive;
    }

    public List<Map<String, Integer>> getEdges() {
        return edges;
    }

    public List<String> getFileIds() {
        return fileIds;
    }

    public List<Integer> getFrameTypes() {
        return frameTypes;
    }

    public List<Boolean> getInlineFrames() {
        return inlineFrames;
    }

    public List<String> getFileNames() {
        return fileNames;
    }

    public List<Integer> getAddressOrLines() {
        return addressOrLines;
    }

    public List<String> getFunctionNames() {
        return functionNames;
    }

    public List<Integer> getFunctionOffsets() {
        return functionOffsets;
    }

    public List<String> getSourceFileNames() {
        return sourceFileNames;
    }

    public List<Integer> getSourceLines() {
        return sourceLines;
    }

    public List<Double> getAnnualCO2TonsInclusive() {
        return annualCO2TonsInclusive;
    }

    public List<Double> getAnnualCostsUSDInclusive() {
        return annualCostsUSDInclusive;
    }

    public long getSelfCPU() {
        return selfCPU;
    }

    public long getTotalCPU() {
        return totalCPU;
    }

    public long getTotalSamples() {
        return totalSamples;
    }

    @UpdateForV9(owner = UpdateForV9.Owner.PROFILING) // change casing from Camel Case to Snake Case (requires updates in Kibana as well)
    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContent.builder(params).object(ob -> {
            ob.array("Edges", edges.iterator(), (eb, e) -> eb.array(intValues(e.values())));
            ob.array("FileID", fileIds.toArray(String[]::new));
            ob.array("FrameType", intValues(frameTypes));
            ob.array("Inline", inlineFrames.iterator(), e -> (b, p) -> b.value(e));
            ob.array("ExeFilename", fileNames.toArray(String[]::new));
            ob.array("AddressOrLine", intValues(addressOrLines));
            ob.array("FunctionName", functionNames.toArray(String[]::new));
            ob.array("FunctionOffset", intValues(functionOffsets));
            ob.array("SourceFilename", sourceFileNames.toArray(String[]::new));
            ob.array("SourceLine", intValues(sourceLines));
            ob.array("CountInclusive", longValues(countInclusive));
            ob.array("CountExclusive", longValues(countExclusive));
            ob.array("AnnualCO2TonsInclusive", doubleValues(annualCO2TonsInclusive));
            ob.array("AnnualCO2TonsExclusive", doubleValues(annualCO2TonsExclusive));
            ob.array("AnnualCostsUSDInclusive", doubleValues(annualCostsUSDInclusive));
            ob.array("AnnualCostsUSDExclusive", doubleValues(annualCostsUSDExclusive));
            ob.field("Size", size);
            ob.field("SamplingRate", samplingRate);
            ob.field("SelfCPU", selfCPU);
            ob.field("TotalCPU", totalCPU);
            ob.field("TotalSamples", totalSamples);
        });
    }

    private static Iterator<ToXContent> intValues(Collection<Integer> values) {
        return Iterators.single((b, p) -> {
            for (Integer i : values) {
                b.value(i);
            }
            return b;
        });
    }

    private static Iterator<ToXContent> longValues(Collection<Long> values) {
        return Iterators.single((b, p) -> {
            for (Long l : values) {
                b.value(l);
            }
            return b;
        });
    }

    private static Iterator<ToXContent> doubleValues(Collection<Double> values) {
        return Iterators.single((b, p) -> {
            for (Double d : values) {
                // write as raw value - we need direct control over the output representation (here: limit to 4 decimal places)
                b.rawValue(NumberUtils.doubleToString(d));
            }
            return b;
        });
    }
}
