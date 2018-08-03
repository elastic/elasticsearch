/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.BinaryProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiFunction;

public abstract class BinaryStringProcessor<O extends Enum<?> & BiFunction<String, T, R>, T, R> extends BinaryProcessor {
    
    private final O operation;
    
    public BinaryStringProcessor(Processor left, Processor right, O operation) {
        super(left, right);
        this.operation = operation;
    }

    public BinaryStringProcessor(StreamInput in, Reader<O> reader) throws IOException {
        super(in);
        operation = reader.read(in);
    }
    
    protected O operation() {
        return operation;
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
        
        BinaryStringProcessor<?,?,?> other = (BinaryStringProcessor<?,?,?>) obj;
        return Objects.equals(operation, other.operation)
                && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }

    @Override
    public String toString() {
        return operation.toString();
    }
}
