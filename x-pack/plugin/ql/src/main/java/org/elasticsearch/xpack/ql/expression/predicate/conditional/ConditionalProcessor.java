/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.conditional;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ConditionalProcessor implements Processor {

    public enum ConditionalOperation implements Function<Collection<Object>, Object> {

        COALESCE(Conditionals::coalesce, Conditionals::coalesceInput),
        GREATEST(Conditionals::greatest, Conditionals::greatestInput),
        LEAST(Conditionals::least, Conditionals::leastInput);


        String scriptMethodName() {
            return name().toLowerCase(Locale.ROOT);
        }

        private final Function<Collection<Object>, Object> process;
        private final BiFunction<List<Processor>, Object, Object> inputProcess;

        ConditionalOperation(Function<Collection<Object>, Object> process,
                             BiFunction<List<Processor>, Object, Object> inputProcess) {
            this.process = process;
            this.inputProcess = inputProcess;
        }

        @Override
        public Object apply(Collection<Object> objects) {
            return process.apply(objects);
        }

        Object applyOnInput(List<Processor> processors, Object input) {
            return inputProcess.apply(processors, input);
        }
    }
    
    public static final String NAME = "nco";

    private final List<Processor> processors;
    private final ConditionalOperation operation;

    public ConditionalProcessor(List<Processor> processors, ConditionalOperation operation) {
        this.processors = processors;
        this.operation = operation;
    }

    public ConditionalProcessor(StreamInput in) throws IOException {
        processors = in.readNamedWriteableList(Processor.class);
        operation = in.readEnum(ConditionalOperation.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(processors);
        out.writeEnum(operation);
    }

    @Override
    public Object process(Object input) {
        return operation.applyOnInput(processors, input);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConditionalProcessor that = (ConditionalProcessor) o;
        return Objects.equals(processors, that.processors) &&
            operation == that.operation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(processors, operation);
    }
}
