/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.util.PlanStreamOutput.writeCachedStringWithVersionCheck;

/**
 * Composite type made of many columns.
 */
record ObjectType(Map<String, AtomType> fields) implements DataType {
    ObjectType(StreamInput in) throws IOException {
        this(in.readImmutableMap(i -> DataType.readFrom(in).atom()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeCachedStringWithVersionCheck(out, "o");
        out.writeMap(fields, (o, t) -> DataType.atom(t).writeTo(o));
    }

    @Override
    public AtomType atom() {
        return AtomType.OBJECT;
    }

    @Override
    public AtomType field(String name) {
        return fields.get(name);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder("{");
        for (Map.Entry<String, AtomType> e : fields.entrySet()) {
            b.append(e.getKey()).append(":").append(e.getValue().typeName()).append(", ");
        }
        return b.toString();
    }
}
