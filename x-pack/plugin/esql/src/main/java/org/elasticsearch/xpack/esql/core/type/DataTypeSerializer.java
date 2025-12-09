/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.Build;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.core.util.PlanStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.io.IOException;

public final class DataTypeSerializer {

    private DataTypeSerializer() {}

    public static void writeTo(DataType datatype, StreamOutput out) throws IOException {
        if (datatype.supportedVersion().supportedOn(out.getTransportVersion(), Build.current().isSnapshot()) == false) {
            /*
             * Throw a 500 error - this is a bug, we failed to account for an old node during planning.
             */
            throw new QlIllegalArgumentException(
                "remote node at version [" + out.getTransportVersion() + "] doesn't understand data type [" + datatype + "]"
            );
        }
        ((PlanStreamOutput) out).writeCachedString(datatype.typeName());
    }

    public static DataType readFrom(StreamInput in) throws IOException {
        return DataType.readFrom(((PlanStreamInput) in).readCachedString());
    }
}
