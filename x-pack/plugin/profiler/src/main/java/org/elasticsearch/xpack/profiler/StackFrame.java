/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

final class StackFrame implements ToXContentObject {
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
            ObjectPath.eval("Stackframe.file.name", source),
            ObjectPath.eval("Stackframe.function.name", source),
            ObjectPath.eval("Stackframe.function.offset", source),
            ObjectPath.eval("Stackframe.line.number", source),
            ObjectPath.eval("Stackframe.source.type", source)
        );
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StackFrame that = (StackFrame) o;
        return Objects.equals(fileName, that.fileName)
            && Objects.equals(functionName, that.functionName)
            && Objects.equals(functionOffset, that.functionOffset)
            && Objects.equals(lineNumber, that.lineNumber)
            && Objects.equals(sourceType, that.sourceType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, functionName, functionOffset, lineNumber, sourceType);
    }
}
