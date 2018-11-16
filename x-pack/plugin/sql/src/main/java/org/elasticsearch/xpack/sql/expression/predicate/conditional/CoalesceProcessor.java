/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class CoalesceProcessor implements Processor {
    
    public static final String NAME = "nco";

    private final List<Processor> processsors;

    public CoalesceProcessor(List<Processor> processors) {
        this.processsors = processors;
    }

    public CoalesceProcessor(StreamInput in) throws IOException {
        processsors = in.readNamedWriteableList(Processor.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(processsors);
    }

    @Override
    public Object process(Object input) {
        for (Processor proc : processsors) {
            Object result = proc.process(input);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    public static Object apply(List<?> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        for (Object object : values) {
            if (object != null) {
                return object;
            }
        }

        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(processsors);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CoalesceProcessor that = (CoalesceProcessor) o;
        return Objects.equals(processsors, that.processsors);
    }
}