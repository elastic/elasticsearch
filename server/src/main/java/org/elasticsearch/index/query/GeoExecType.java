/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/** Specifies how a geo query should be run. */
public enum GeoExecType implements Writeable {

    MEMORY(0), INDEXED(1);

    private final int ordinal;

    GeoExecType(int ordinal) {
        this.ordinal = ordinal;
    }

    public static GeoExecType readFromStream(StreamInput in) throws IOException {
        int ord = in.readVInt();
        switch(ord) {
            case(0): return MEMORY;
            case(1): return INDEXED;
        }
        throw new ElasticsearchException("unknown serialized type [" + ord + "]");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.ordinal);
    }

    public static GeoExecType fromString(String typeName) {
        if (typeName == null) {
            throw new IllegalArgumentException("cannot parse type from null string");
        }

        for (GeoExecType type : GeoExecType.values()) {
            if (type.name().equalsIgnoreCase(typeName)) {
                return type;
            }
        }
        throw new IllegalArgumentException("no type can be parsed from ordinal " + typeName);
    }
}
