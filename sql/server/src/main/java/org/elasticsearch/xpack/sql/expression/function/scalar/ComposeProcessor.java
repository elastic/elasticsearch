/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@linkplain ColumnProcessor} that composes the results of two
 * {@linkplain ColumnProcessor}s.
 */
public class ComposeProcessor implements ColumnProcessor {
    static final String NAME = ".";
    private final ColumnProcessor first;
    private final ColumnProcessor second;

    public ComposeProcessor(ColumnProcessor first, ColumnProcessor second) {
        this.first = first;
        this.second = second;
    }

    public ComposeProcessor(StreamInput in) throws IOException {
        first = in.readNamedWriteable(ColumnProcessor.class);
        second = in.readNamedWriteable(ColumnProcessor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(first);
        out.writeNamedWriteable(second);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object apply(Object r) {
        return second.apply(first.apply(r));
    }

    ColumnProcessor first() {
        return first;
    }

    ColumnProcessor second() {
        return second;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ComposeProcessor other = (ComposeProcessor) obj;
        return first.equals(other.first)
                && second.equals(other.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        // borrow Haskell's notation for function comosition
        return "(" + second + " . " + first + ")";
    }
}
