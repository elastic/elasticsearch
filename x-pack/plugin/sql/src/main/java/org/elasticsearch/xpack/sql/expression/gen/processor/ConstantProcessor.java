/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.processor;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class ConstantProcessor implements Processor {

    public static String NAME = "c";

    private final Object constant;
    private final boolean namedWriteable;
    private final Class<?> clazz;

    public ConstantProcessor(Object value) {
        this.constant = value;
        this.namedWriteable = value instanceof NamedWriteable;
        this.clazz = namedWriteable ? value.getClass() : null;
    }

    @SuppressWarnings("unchecked")
    public ConstantProcessor(StreamInput in) throws IOException {
        namedWriteable = in.readBoolean();
        if (namedWriteable) {
            try {
                clazz = ConstantProcessor.class.getClassLoader().loadClass(in.readString());
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
            constant = in.readNamedWriteable((Class<NamedWriteable>) clazz);
        } else {
            clazz = null;
            constant = in.readGenericValue();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(namedWriteable);
        if (namedWriteable) {
            out.writeString(constant.getClass().getName());
            out.writeNamedWriteable((NamedWriteable) constant);
        } else {
            out.writeGenericValue(constant);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        return constant;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(constant);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ConstantProcessor other = (ConstantProcessor) obj;
        return Objects.equals(constant, other.constant);
    }

    @Override
    public String toString() {
        return "^" + constant;
    }
}
