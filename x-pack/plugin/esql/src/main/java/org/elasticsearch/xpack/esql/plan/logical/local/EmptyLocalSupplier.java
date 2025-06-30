/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;

import java.io.IOException;

public class EmptyLocalSupplier implements LocalSupplier {

    public static final LocalSupplier EMPTY = new EmptyLocalSupplier();
    public static final String NAME = "EmptySupplier";
    private static final Writeable.Reader<LocalSupplier> EMPTY_LOCAL_SUPPLIER_READER = in -> {
        assert in.readVInt() == 0;
        return EMPTY;
    };
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LocalSupplier.class,
        NAME,
        EMPTY_LOCAL_SUPPLIER_READER
    );

    private EmptyLocalSupplier() {}

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Block[] get() {
        return BlockUtils.NO_BLOCKS;
    }

    @Override
    public String toString() {
        return "EMPTY";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(0);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == EMPTY;
    }

    @Override
    public int hashCode() {
        return 0;
    }

}
