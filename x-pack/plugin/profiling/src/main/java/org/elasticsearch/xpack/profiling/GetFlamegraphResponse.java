/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GetFlamegraphResponse extends ActionResponse implements ChunkedToXContentObject {
    private final int size;
    private final double samplingRate;
    private final long selfCPU;
    private final long totalCPU;
    private final double selfAnnualCO2Tons;
    private final double totalAnnualCO2Tons;
    private final double selfAnnualCostsUSD;
    private final double totalAnnualCostsUSD;
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

    public GetFlamegraphResponse(StreamInput in) throws IOException {
        this.size = in.readInt();
        this.samplingRate = in.readDouble();
        this.edges = in.readCollectionAsList(i -> i.readMap(StreamInput::readInt));
        this.fileIds = in.readCollectionAsList(StreamInput::readString);
        this.frameTypes = in.readCollectionAsList(StreamInput::readInt);
        this.inlineFrames = in.readCollectionAsList(StreamInput::readBoolean);
        this.fileNames = in.readCollectionAsList(StreamInput::readString);
        this.addressOrLines = in.readCollectionAsList(StreamInput::readInt);
        this.functionNames = in.readCollectionAsList(StreamInput::readString);
        this.functionOffsets = in.readCollectionAsList(StreamInput::readInt);
        this.sourceFileNames = in.readCollectionAsList(StreamInput::readString);
        this.sourceLines = in.readCollectionAsList(StreamInput::readInt);
        this.countInclusive = in.readCollectionAsList(StreamInput::readLong);
        this.countExclusive = in.readCollectionAsList(StreamInput::readLong);
        this.annualCO2TonsInclusive = in.readCollectionAsList(StreamInput::readDouble);
        this.annualCO2TonsExclusive = in.readCollectionAsList(StreamInput::readDouble);
        this.annualCostsUSDInclusive = in.readCollectionAsList(StreamInput::readDouble);
        this.annualCostsUSDExclusive = in.readCollectionAsList(StreamInput::readDouble);
        this.selfCPU = in.readLong();
        this.totalCPU = in.readLong();
        this.selfAnnualCO2Tons = in.readDouble();
        this.totalAnnualCO2Tons = in.readDouble();
        this.selfAnnualCostsUSD = in.readDouble();
        this.totalAnnualCostsUSD = in.readDouble();
        this.totalSamples = in.readLong();
    }

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
        double selfAnnualCO2Tons,
        double totalAnnualCO2Tons,
        double selfAnnualCostsUSD,
        double totalAnnualCostsUSD,
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
        this.selfAnnualCO2Tons = selfAnnualCO2Tons;
        this.totalAnnualCO2Tons = totalAnnualCO2Tons;
        this.selfAnnualCostsUSD = selfAnnualCostsUSD;
        this.totalAnnualCostsUSD = totalAnnualCostsUSD;
        this.totalSamples = totalSamples;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(this.size);
        out.writeDouble(this.samplingRate);
        out.writeCollection(this.edges, (o, v) -> o.writeMap(v, StreamOutput::writeString, StreamOutput::writeInt));
        out.writeCollection(this.fileIds, StreamOutput::writeString);
        out.writeCollection(this.frameTypes, StreamOutput::writeInt);
        out.writeCollection(this.inlineFrames, StreamOutput::writeBoolean);
        out.writeCollection(this.fileNames, StreamOutput::writeString);
        out.writeCollection(this.addressOrLines, StreamOutput::writeInt);
        out.writeCollection(this.functionNames, StreamOutput::writeString);
        out.writeCollection(this.functionOffsets, StreamOutput::writeInt);
        out.writeCollection(this.sourceFileNames, StreamOutput::writeString);
        out.writeCollection(this.sourceLines, StreamOutput::writeInt);
        out.writeCollection(this.countInclusive, StreamOutput::writeLong);
        out.writeCollection(this.countExclusive, StreamOutput::writeLong);
        out.writeCollection(this.annualCO2TonsInclusive, StreamOutput::writeDouble);
        out.writeCollection(this.annualCO2TonsExclusive, StreamOutput::writeDouble);
        out.writeCollection(this.annualCostsUSDInclusive, StreamOutput::writeDouble);
        out.writeCollection(this.annualCostsUSDExclusive, StreamOutput::writeDouble);
        out.writeLong(this.selfCPU);
        out.writeLong(this.totalCPU);
        out.writeDouble(this.selfAnnualCO2Tons);
        out.writeDouble(this.totalAnnualCO2Tons);
        out.writeDouble(this.selfAnnualCostsUSD);
        out.writeDouble(this.totalAnnualCostsUSD);
        out.writeLong(this.totalSamples);
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

    public long getSelfCPU() {
        return selfCPU;
    }

    public long getTotalCPU() {
        return totalCPU;
    }

    public long getTotalSamples() {
        return totalSamples;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            ChunkedToXContentHelper.array(
                "Edges",
                Iterators.flatMap(
                    edges.iterator(),
                    perNodeEdges -> Iterators.concat(
                        ChunkedToXContentHelper.startArray(),
                        Iterators.map(perNodeEdges.entrySet().iterator(), edge -> (b, p) -> b.value(edge.getValue())),
                        ChunkedToXContentHelper.endArray()
                    )
                )
            ),
            ChunkedToXContentHelper.array("FileID", Iterators.map(fileIds.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("FrameType", Iterators.map(frameTypes.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("Inline", Iterators.map(inlineFrames.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("ExeFilename", Iterators.map(fileNames.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("AddressOrLine", Iterators.map(addressOrLines.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("FunctionName", Iterators.map(functionNames.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("FunctionOffset", Iterators.map(functionOffsets.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("SourceFilename", Iterators.map(sourceFileNames.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("SourceLine", Iterators.map(sourceLines.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("CountInclusive", Iterators.map(countInclusive.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("CountExclusive", Iterators.map(countExclusive.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array(
                "AnnualCO2TonsInclusive",
                Iterators.map(annualCO2TonsInclusive.iterator(), e -> (b, p) -> b.value(e))
            ),
            ChunkedToXContentHelper.array(
                "AnnualCO2TonsExclusive",
                Iterators.map(annualCO2TonsExclusive.iterator(), e -> (b, p) -> b.value(e))
            ),
            ChunkedToXContentHelper.array(
                "AnnualCostsUSDInclusive",
                Iterators.map(annualCostsUSDInclusive.iterator(), e -> (b, p) -> b.value(e))
            ),
            ChunkedToXContentHelper.array(
                "AnnualCostsUSDExclusive",
                Iterators.map(annualCostsUSDExclusive.iterator(), e -> (b, p) -> b.value(e))
            ),
            Iterators.single((b, p) -> b.field("Size", size)),
            Iterators.single((b, p) -> b.field("SamplingRate", samplingRate)),
            Iterators.single((b, p) -> b.field("SelfCPU", selfCPU)),
            Iterators.single((b, p) -> b.field("TotalCPU", totalCPU)),
            Iterators.single((b, p) -> b.field("SelfAnnualCO2Tons", selfAnnualCO2Tons)),
            Iterators.single((b, p) -> b.field("TotalAnnualCO2Tons", totalAnnualCO2Tons)),
            Iterators.single((b, p) -> b.field("SelfAnnualCostsUSD", selfAnnualCostsUSD)),
            Iterators.single((b, p) -> b.field("TotalAnnualCostsUSD", totalAnnualCostsUSD)),
            Iterators.single((b, p) -> b.field("TotalSamples", totalSamples)),
            ChunkedToXContentHelper.endObject()
        );
    }
}
