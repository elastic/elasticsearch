/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Convenient base class for any processor that combines many processors into one.
 */
abstract public class VarArgsProcessor implements Processor {

    private static List<Processor> read(StreamInput in) throws IOException {
        final int count = in.readInt();
        final List<Processor> processors = new ArrayList<>(count);

        for(int i = 0; i < count; i++) {
            processors.add(in.readNamedWriteable(Processor.class));
        }

        return processors;
    }

    private final List<Processor> processors;

    protected VarArgsProcessor(final List<Processor> processors) {
        this.processors = processors;
    }

    protected VarArgsProcessor(StreamInput in) throws IOException {
        this(read(in));
    }

    @Override
    final public Object process(Object input) {
        return input == null ?
            null :
            this.doProcess(this.processors.stream()
                .map(p -> p.process(input))
                .collect(Collectors.toList()));
    }

    abstract protected Object doProcess(final List<Object> inputs);

    @Override
    final public void writeTo(StreamOutput out) throws IOException {
        final List<Processor> processors = this.processors();
        out.writeInt(processors.size());

        for(Processor p : processors) {
            out.writeNamedWriteable(p);
        }

        this.writeToAdditional(out);
    }

    protected abstract void writeToAdditional(StreamOutput out) throws IOException;

    protected List<Processor> processors() {
        return this.processors;
    }

    @Override
    final public int hashCode() {
        return this.hashCode(this.processors);
    }

    protected abstract int hashCode(final List<Processor> processors);

    @Override
    public boolean equals(Object other) {
        if (other == null || other.getClass() != getClass()) {
            return false;
        }
        VarArgsProcessor otherProcessor = (VarArgsProcessor) other;
        return Objects.equals(this.processors, otherProcessor.processors)
            && equals0(otherProcessor);
    }

    protected abstract boolean equals0(VarArgsProcessor other);
}
