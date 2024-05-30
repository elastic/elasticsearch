/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public interface DataType extends Writeable {
    static DataType readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        if (name.equalsIgnoreCase(DataTypes.DOC_DATA_TYPE.name())) {
            return DataTypes.DOC_DATA_TYPE;
        }
        DataType dataType = DataTypes.fromTypeName(name);
        if (dataType == null) {
            throw new IOException("Unknown DataType for type name: " + name);
        }
        return dataType;
    }

    String name();

    String typeName();

    String esType();

    boolean isInteger();

    boolean isRational();

    boolean isNumeric();

    int size();

    boolean hasDocValues();

    @Override
    int hashCode();

    @Override
    boolean equals(Object obj);

    @Override
    String toString();

    @Override
    void writeTo(StreamOutput out) throws IOException;
}
