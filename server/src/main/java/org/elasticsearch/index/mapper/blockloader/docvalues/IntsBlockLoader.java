/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

/**
 * Loads {@code int}s from doc values.
 */
public class IntsBlockLoader extends AbstractNumericBlockLoader<BlockLoader.IntBuilder> {
    public IntsBlockLoader(String fieldName) {
        this(fieldName, false);
    }

    public IntsBlockLoader(String fieldName, boolean readInArrayOrder) {
        super(fieldName, "IntsFromDocValues", readInArrayOrder);
    }

    @Override
    public IntBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.ints(expectedCount);
    }

    @Override
    protected IntBuilder newBuilder(BlockFactory factory, int expectedCount) {
        return factory.intsFromDocValues(expectedCount);
    }

    @Override
    protected void appendValue(IntBuilder builder, long rawValue) {
        builder.appendInt(Math.toIntExact(rawValue));
    }

    @Override
    protected Block tryDirectRead(OptionalColumnAtATimeReader direct, BlockFactory factory, Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        return direct.tryRead(factory, docs, offset, nullsFiltered, null, true, false);
    }

    @Override
    public String toString() {
        return "IntsFromDocValues[" + fieldName + "]";
    }
}
