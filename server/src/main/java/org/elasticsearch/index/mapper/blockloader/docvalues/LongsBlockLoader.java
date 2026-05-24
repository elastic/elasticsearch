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

public class LongsBlockLoader extends AbstractNumericBlockLoader<BlockLoader.LongBuilder> {
    public LongsBlockLoader(String fieldName) {
        this(fieldName, false);
    }

    public LongsBlockLoader(String fieldName, boolean readInArrayOrder) {
        super(fieldName, "LongsFromDocValues", readInArrayOrder);
    }

    @Override
    public LongBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.longs(expectedCount);
    }

    @Override
    protected LongBuilder newBuilder(BlockFactory factory, int expectedCount) {
        return factory.longsFromDocValues(expectedCount);
    }

    @Override
    protected void appendValue(LongBuilder builder, long rawValue) {
        builder.appendLong(rawValue);
    }

    @Override
    protected Block tryDirectRead(OptionalColumnAtATimeReader direct, BlockFactory factory, Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        return direct.tryRead(factory, docs, offset, nullsFiltered, null, false, false);
    }

    @Override
    public String toString() {
        return "LongsFromDocValues[" + fieldName + "]";
    }
}
