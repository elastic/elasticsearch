/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.type.DataType;

import java.io.IOException;

/**
 * A column of data provided in the request.
 */
public record Column(DataType type, Block values) implements Releasable, Writeable {
    public Column(BlockStreamInput in) throws IOException {
        this(EsqlDataTypes.fromTypeName(in.readString()), in.readNamedWriteable(Block.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type.typeName());
        out.writeNamedWriteable(values);
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(values);
    }
}
