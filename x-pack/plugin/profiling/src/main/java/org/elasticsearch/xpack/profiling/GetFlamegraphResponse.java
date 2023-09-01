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
    private final double totalSeconds;
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

    public GetFlamegraphResponse(StreamInput in) throws IOException {
        this.size = in.readInt();
        this.samplingRate = in.readDouble();
        this.totalSeconds = in.readDouble();
        this.edges = in.readList(i -> i.readMap(StreamInput::readInt));
        this.fileIds = in.readStringList();
        this.frameTypes = in.readList(StreamInput::readInt);
        this.inlineFrames = in.readList(StreamInput::readBoolean);
        this.fileNames = in.readStringList();
        this.addressOrLines = in.readList(StreamInput::readInt);
        this.functionNames = in.readStringList();
        this.functionOffsets = in.readList(StreamInput::readInt);
        this.sourceFileNames = in.readStringList();
        this.sourceLines = in.readList(StreamInput::readInt);
        this.countInclusive = in.readList(StreamInput::readInt);
        this.countExclusive = in.readList(StreamInput::readInt);
    }

    public GetFlamegraphResponse(
        int size,
        double samplingRate,
        double totalSeconds,
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
        List<Integer> countInclusive,
        List<Integer> countExclusive
    ) {
        this.size = size;
        this.samplingRate = samplingRate;
        this.totalSeconds = totalSeconds;
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(this.size);
        out.writeDouble(this.samplingRate);
        out.writeDouble(this.totalSeconds);
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
        out.writeCollection(this.countInclusive, StreamOutput::writeInt);
        out.writeCollection(this.countExclusive, StreamOutput::writeInt);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            ChunkedToXContentHelper.array("edges", Iterators.flatMap(edges.iterator(), perNodeEdges -> {
                // returns the full map but looking at the loop at the end of createBaseFlameGraph() we're actually only interested in
                // the values anyway. Ask Joseph why we need the intermediary map anyway.
                return Iterators.concat(
                    ChunkedToXContentHelper.startArray(),
                    Iterators.map(perNodeEdges.entrySet().iterator(), edge -> (b, p) -> {
                        return b.value(edge.getValue());
                    }),
                    ChunkedToXContentHelper.endArray()
                );
                /*
                return Iterators.concat(
                    ChunkedToXContentHelper.startObject(),
                    Iterators.map(perNodeEdges.entrySet().iterator(), edge -> (b, p) -> {
                        return b.field(edge.getKey(), edge.getValue());
                    }),
                    ChunkedToXContentHelper.endObject()
                );
                 */
            })),
            ChunkedToXContentHelper.array("fileIds", Iterators.map(fileIds.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("frameTypes", Iterators.map(frameTypes.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("inlineFrames", Iterators.map(inlineFrames.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("fileNames", Iterators.map(fileNames.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("addressOrLines", Iterators.map(addressOrLines.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("functionNames", Iterators.map(functionNames.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("functionOffsets", Iterators.map(functionOffsets.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("sourceFileNames", Iterators.map(sourceFileNames.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("sourceLines", Iterators.map(sourceLines.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("countInclusive", Iterators.map(countInclusive.iterator(), e -> (b, p) -> b.value(e))),
            ChunkedToXContentHelper.array("countExclusive", Iterators.map(countExclusive.iterator(), e -> (b, p) -> b.value(e))),
            Iterators.single((b, p) -> b.field("size", size)),
            Iterators.single((b, p) -> b.field("samplingRate", samplingRate)),
            Iterators.single((b, p) -> b.field("totalSeconds", totalSeconds)),
            ChunkedToXContentHelper.endObject()
        );
    }
}
