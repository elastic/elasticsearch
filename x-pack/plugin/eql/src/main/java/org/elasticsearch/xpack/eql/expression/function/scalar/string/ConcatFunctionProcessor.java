/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ConcatFunctionProcessor implements Processor {

    public static final String NAME = "scon";

    private final List<Processor> values;

    public ConcatFunctionProcessor(List<Processor> values) {
        this.values = values;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        for (Processor v: values) {
            out.writeNamedWriteable(v);
        }
    }

    @Override
    public Object process(Object input) {
        List<Object> processed = new ArrayList<>(values.size());
        for (Processor v: values) {
            processed.add(v.process(input));
        }
        return doProcess(processed);
    }

    public static Object doProcess(List<Object> inputs) {
        if (inputs == null) {
            return null;
        }

        StringBuilder str = new StringBuilder();

        for (Object input: inputs) {
            if (input == null) {
                return null;
            }

            str.append(input.toString());
        }

        return str.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return Objects.equals(values, ((ConcatFunctionProcessor) obj).values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }
}
