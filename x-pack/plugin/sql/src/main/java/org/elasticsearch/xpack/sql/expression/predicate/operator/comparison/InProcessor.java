/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class InProcessor implements Processor {

    public static final String NAME = "in";

    private final List<Processor> processsors;

    public InProcessor(List<Processor> processors) {
        this.processsors = processors;
    }

    public InProcessor(StreamInput in) throws IOException {
        processsors = in.readNamedWriteableList(Processor.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(processsors);
    }

    @Override
    public Object process(Object input) {
        Object leftValue = processsors.get(processsors.size() - 1).process(input);

        for (int i = 0; i < processsors.size() - 1; i++) {
            Boolean compResult = Comparisons.eq(leftValue, processsors.get(i).process(input));
            if (compResult != null && compResult) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InProcessor that = (InProcessor) o;
        return Objects.equals(processsors, that.processsors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processsors);
    }
}
