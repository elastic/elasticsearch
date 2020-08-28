/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.gen.processor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@linkplain Processor} that composes the results of two
 * {@linkplain Processor}s.
 */
public class ChainingProcessor extends UnaryProcessor {
    public static final String NAME = ".";

    private final Processor processor;

    public ChainingProcessor(Processor first, Processor second) {
        super(first);
        this.processor = second;
    }

    public ChainingProcessor(StreamInput in) throws IOException {
        super(in);
        processor = in.readNamedWriteable(Processor.class);
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {
        out.writeNamedWriteable(processor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object input) {
        return processor.process(input);
    }

    Processor first() {
        return child();
    }

    Processor second() {
        return processor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), processor);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(processor, ((ChainingProcessor) obj).processor);
    }

    @Override
    public String toString() {
        return processor + "(" + super.toString() + ")";
    }
}