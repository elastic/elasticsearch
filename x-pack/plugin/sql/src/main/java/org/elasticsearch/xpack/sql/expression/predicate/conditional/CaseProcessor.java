/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class CaseProcessor implements Processor {

    public static final String NAME = "case";

    private final List<Processor> processors;

    public CaseProcessor(List<Processor> processors) {
        this.processors = processors;
    }

    public CaseProcessor(StreamInput in) throws IOException {
        processors = in.readNamedWriteableList(Processor.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(processors);
    }

    @Override
    public Object process(Object input) {
        // Check every condition in sequence and if it evaluates to TRUE,
        // evaluate and return the result associated with that condition.
        for (int i = 0; i < processors.size() - 2; i += 2) {
            if (processors.get(i).process(input) == Boolean.TRUE) {
                return processors.get(i + 1).process(input);
            }
        }
        // resort to default value
        return processors.get(processors.size() - 1).process(input);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CaseProcessor that = (CaseProcessor) o;
        return Objects.equals(processors, that.processors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processors);
    }
}
