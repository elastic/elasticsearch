/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.processor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public abstract class UnaryProcessor implements Processor {

    private final Processor child;

    public UnaryProcessor(Processor child) {
        this.child = child;
    }

    protected UnaryProcessor(StreamInput in) throws IOException {
        child = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(child);
        doWrite(out);
    }

    protected abstract void doWrite(StreamOutput out) throws IOException;

    @Override
    public final Object process(Object input) {
        return doProcess(child.process(input));
    }

    public Processor child() {
        return child;
    }

    protected abstract Object doProcess(Object input);

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnaryProcessor other = (UnaryProcessor) obj;
        return Objects.equals(child, other.child);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(child);
    }

    @Override
    public String toString() {
        return Objects.toString(child);
    }
}