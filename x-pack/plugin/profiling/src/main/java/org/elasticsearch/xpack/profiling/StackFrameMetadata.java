/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

final class StackFrameMetadata implements ToXContentObject {
    String frameID;
    String fileID;
    int frameType;
    boolean inline;
    int addressOrLine;
    String functionName;
    int functionOffset;
    String sourceFilename;
    int sourceLine;
    String exeFilename;

    StackFrameMetadata(
        String frameID,
        String fileID,
        int frameType,
        boolean inline,
        int addressOrLine,
        String functionName,
        int functionOffset,
        String sourceFilename,
        int sourceLine,
        String exeFilename
    ) {
        this.frameID = frameID;
        this.fileID = fileID;
        this.frameType = frameType;
        this.inline = inline;
        this.addressOrLine = addressOrLine;
        this.functionName = functionName;
        this.functionOffset = functionOffset;
        this.sourceFilename = sourceFilename;
        this.sourceLine = sourceLine;
        this.exeFilename = exeFilename;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("FrameID", this.frameID);
        builder.field("FileID", this.fileID);
        builder.field("FrameType", this.frameType);
        builder.field("Inline", this.inline);
        builder.field("AddressOrLine", this.addressOrLine);
        builder.field("FunctionName", this.functionName);
        builder.field("FunctionOffset", this.functionOffset);
        builder.field("SourceFilename", this.sourceFilename);
        builder.field("SourceLine", this.sourceLine);
        builder.field("ExeFileName", this.exeFilename);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StackFrameMetadata that = (StackFrameMetadata) o;
        return Objects.equals(frameID, that.frameID)
            && Objects.equals(fileID, that.fileID)
            && Objects.equals(frameType, that.frameType)
            && Objects.equals(inline, that.inline)
            && Objects.equals(addressOrLine, that.addressOrLine)
            && Objects.equals(functionName, that.functionName)
            && Objects.equals(functionOffset, that.functionOffset)
            && Objects.equals(sourceFilename, that.sourceFilename)
            && Objects.equals(sourceLine, that.sourceLine)
            && Objects.equals(exeFilename, that.exeFilename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            frameID,
            fileID,
            frameType,
            inline,
            addressOrLine,
            functionName,
            functionOffset,
            sourceFilename,
            sourceLine,
            exeFilename
        );
    }
}
