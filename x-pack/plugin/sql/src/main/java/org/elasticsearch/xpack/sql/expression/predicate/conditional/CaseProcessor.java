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
import java.util.ArrayList;
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
        List<Object> objects = new ArrayList<>(processors.size());
        for (Processor processor : processors) {
            objects.add(processor.process(input));
        }
        return apply(objects);
    }

    public static Object apply(List<Object> objects) {
        for (int i = 0; i < objects.size() - 2; i += 2) {
            if (objects.get(i) == Boolean.TRUE) {
                return objects.get(i + 1);
            }
        }
        // resort to default value
        return objects.get(objects.size() - 1);
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
