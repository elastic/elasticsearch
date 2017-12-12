/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class MatrixFieldProcessor implements Processor {
    public static final String NAME = "mat";

    private final String key;

    public MatrixFieldProcessor(String key) {
        this.key = key;
    }

    public MatrixFieldProcessor(StreamInput in) throws IOException {
        key = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    String key() {
        return key;
    }

    @Override
    public Object process(Object r) {
        return r instanceof Map ? ((Map<?, ?>) r).get(key) : r;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MatrixFieldProcessor other = (MatrixFieldProcessor) obj;
        return Objects.equals(key, other.key);
    }

    public String toString() {
        return "[" + key + "]";
    }
}