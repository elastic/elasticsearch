/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.profiler.utils.MapExtractor;

import java.io.IOException;
import java.util.Map;

final class StackFrame implements ToXContent {
    String fileName;
    String functionName;
    Integer functionOffset;
    Integer lineNumber;
    Integer sourceType;

    StackFrame(String fileName, String functionName, Integer functionOffset, Integer lineNumber, Integer sourceType) {
        this.fileName = fileName;
        this.functionName = functionName;
        this.functionOffset = functionOffset;
        this.lineNumber = lineNumber;
        this.sourceType = sourceType;
    }

    public static StackFrame fromSource(Map<String, Object> source) {
        return new StackFrame(
            MapExtractor.read(source, "Stackframe", "file", "name"),
            MapExtractor.read(source, "Stackframe", "function", "name"),
            MapExtractor.read(source, "Stackframe", "function", "offset"),
            MapExtractor.read(source, "Stackframe", "line", "number"),
            MapExtractor.read(source, "Stackframe", "source", "type")
        );
    }

    @Override
    public String toString() {
        return "StackFrame{"
            + "fileName='"
            + fileName
            + '\''
            + ", functionName='"
            + functionName
            + '\''
            + ", functionOffset="
            + functionOffset
            + ", lineNumber="
            + lineNumber
            + ", sourceType="
            + sourceType
            + '}';
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("file_name", this.fileName);
        builder.field("function_name", this.functionName);
        builder.field("function_offset", this.functionOffset);
        builder.field("line_number", this.lineNumber);
        builder.field("source_type", this.sourceType);
        builder.endObject();
        return builder;
    }
}
