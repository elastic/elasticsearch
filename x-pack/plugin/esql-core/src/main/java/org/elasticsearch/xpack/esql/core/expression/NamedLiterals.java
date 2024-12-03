/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;

/**
 * Represent a collect of key-value pairs as function arguments.
 */
public class NamedLiterals extends Literal {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "NamedLiteras",
        NamedLiterals::readFrom
    );

    private final Map<String, String> args;

    public NamedLiterals(Source source, Map<String, String> args) {
        super(source, args, UNSUPPORTED);
        this.args = args;
    }

    private static NamedLiterals readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((StreamInput & PlanStreamInput) in);
        Map<String, String> args = in.readMap(StreamInput::readString, StreamInput::readString);
        return new NamedLiterals(source, args);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeMap(args, StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends NamedLiterals> info() {
        return NodeInfo.create(this, NamedLiterals::new, args);
    }

    public Map<String, String> args() {
        return args;
    }

    @Override
    public DataType dataType() {
        return UNSUPPORTED;
    }

    @Override
    public Object fold() {
        return args;
    }

    @Override
    public int hashCode() {
        return Objects.hash(args);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        NamedLiterals other = (NamedLiterals) obj;
        return Objects.equals(args, other.args);
    }

    @Override
    public String toString() {
        return String.valueOf(args);
    }

    @Override
    public String nodeString() {
        return toString();
    }

}
