/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.gen.processor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * Base class for definition binary processors based on functions (for applying) defined as enums (for serialization purposes).
 */
public abstract class FunctionalEnumBinaryProcessor<T, U, R, F extends Enum<F> & BiFunction<T, U, R>> extends FunctionalBinaryProcessor<
    T,
    U,
    R,
    F> {

    protected FunctionalEnumBinaryProcessor(Processor left, Processor right, F function) {
        super(left, right, function);
    }

    protected FunctionalEnumBinaryProcessor(StreamInput in, Reader<F> reader) throws IOException {
        super(in, reader);
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {
        out.writeEnum(function());
    }
}
