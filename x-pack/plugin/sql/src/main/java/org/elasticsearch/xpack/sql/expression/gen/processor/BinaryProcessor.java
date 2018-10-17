/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.processor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class BinaryProcessor implements Processor {

    private final Processor left, right;

    public BinaryProcessor(Processor left, Processor right) {
        this.left = left;
        this.right = right;
    }

    protected BinaryProcessor(StreamInput in) throws IOException {
        left = in.readNamedWriteable(Processor.class);
        right = in.readNamedWriteable(Processor.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(left);
        out.writeNamedWriteable(right);
        doWrite(out);
    }

    protected abstract void doWrite(StreamOutput out) throws IOException;

    @Override
    public Object process(Object input) {
        return doProcess(left.process(input), right.process(input));
    }

    protected Processor left() {
        return left;
    }

    protected Processor right() {
        return right;
    }

    protected abstract Object doProcess(Object left, Object right);
}
