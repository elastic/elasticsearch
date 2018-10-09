/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.gen.processor.BinaryProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;

public abstract class BinaryNumericProcessor<O extends Enum<?> & BiFunction<Number, Number, Number>> extends BinaryProcessor {
    
    private final O operation;

    protected BinaryNumericProcessor(Processor left, Processor right, O operation) {
        super(left, right);
        this.operation = operation;
    }

    protected BinaryNumericProcessor(StreamInput in, Reader<O> reader) throws IOException {
        super(in);
        operation = reader.read(in);
    }

    protected O operation() {
        return operation;
    }

    @Override
    protected Object doProcess(Object left, Object right) {
        if (left == null || right == null) {
            return null;
        }
        if (!(left instanceof Number)) {
            throw new SqlIllegalArgumentException("A number is required; received {}", left);
        }
        if (!(right instanceof Number)) {
            throw new SqlIllegalArgumentException("A number is required; received {}", right);
        }

        return operation.apply((Number) left, (Number) right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        BinaryNumericProcessor<?> other = (BinaryNumericProcessor<?>) obj;
        return Objects.equals(operation, other.operation)
                && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "(%s %s %s)", left(), operation, right());
    }
}