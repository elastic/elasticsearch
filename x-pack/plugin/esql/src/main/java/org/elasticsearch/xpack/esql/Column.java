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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;

/**
 * A "column" from a {@code table} provided in the request.
 */
public record Column(DataType type, Block values) implements Releasable, Writeable {
    public Column {
        assert PlannerUtils.toElementType(type) == values.elementType();
    }

    public Column(BlockStreamInput in) throws IOException {
        this(DataType.fromTypeName(in.readString()), Block.readTypedBlock(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type.typeName());
        Block.writeTypedBlock(values, out);
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(values);
    }
}
