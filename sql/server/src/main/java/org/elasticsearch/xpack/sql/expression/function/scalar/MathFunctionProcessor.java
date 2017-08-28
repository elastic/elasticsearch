/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor;

import java.io.IOException;

public class MathFunctionProcessor implements ColumnProcessor {
    public static final String NAME = "m";

    private final MathProcessor processor;

    public MathFunctionProcessor(MathProcessor processor) {
        this.processor = processor;
    }

    MathFunctionProcessor(StreamInput in) throws IOException {
        processor = in.readEnum(MathProcessor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(processor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object apply(Object r) {
        return processor.apply(r);
    }

    MathProcessor processor() {
        return processor;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MathFunctionProcessor other = (MathFunctionProcessor) obj;
        return processor == other.processor;
    }

    @Override
    public int hashCode() {
        return processor.hashCode();
    }

    @Override
    public String toString() {
        return processor.toString();
    }
}
