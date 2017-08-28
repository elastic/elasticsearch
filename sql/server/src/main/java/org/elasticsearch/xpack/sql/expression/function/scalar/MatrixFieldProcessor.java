/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

public class MatrixFieldProcessor implements ColumnProcessor {
    public static final String NAME = "mat";

    private final String key;

    public MatrixFieldProcessor(String key) {
        this.key = key;
    }

    MatrixFieldProcessor(StreamInput in) throws IOException {
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
    public Object apply(Object r) {
        return r instanceof Map ? ((Map<?, ?>) r).get(key) : r;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MatrixFieldProcessor other = (MatrixFieldProcessor) obj;
        return key.equals(other.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    public String toString() {
        return "[" + key + "]";
    }
}
