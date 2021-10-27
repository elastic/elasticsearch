/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.gen.processor;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Base class for definition binary processors based on functions (for applying).
 */
public abstract class FunctionalBinaryProcessor<T, U, R, F extends BiFunction<T, U, R>> extends BinaryProcessor {

    private final F function;

    protected FunctionalBinaryProcessor(Processor left, Processor right, F function) {
        super(left, right);
        this.function = function;
    }

    protected FunctionalBinaryProcessor(StreamInput in, Reader<F> reader) throws IOException {
        super(in);
        this.function = reader.read(in);
    }

    public F function() {
        return function;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object doProcess(Object left, Object right) {
        return function.apply((T) left, (U) right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right(), function());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FunctionalBinaryProcessor<?, ?, ?, ?> other = (FunctionalBinaryProcessor<?, ?, ?, ?>) obj;
        return Objects.equals(function(), other.function())
            && Objects.equals(left(), other.left())
            && Objects.equals(right(), other.right());
    }
}
