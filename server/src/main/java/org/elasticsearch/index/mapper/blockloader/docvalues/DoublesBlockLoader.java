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

public class DoublesBlockLoader extends AbstractNumericBlockLoader<BlockLoader.DoubleBuilder> {
    protected final BlockDocValuesReader.ToDouble toDouble;

    public DoublesBlockLoader(String fieldName, BlockDocValuesReader.ToDouble toDouble) {
        this(fieldName, toDouble, false);
    }

    public DoublesBlockLoader(String fieldName, BlockDocValuesReader.ToDouble toDouble, boolean readInArrayOrder) {
        super(fieldName, "DoublesFromDocValues", readInArrayOrder);
        this.toDouble = toDouble;
    }

    @Override
    public DoubleBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.doubles(expectedCount);
    }

    @Override
    protected DoubleBuilder newBuilder(BlockFactory factory, int expectedCount) {
        return factory.doublesFromDocValues(expectedCount);
    }

    @Override
    protected void appendValue(DoubleBuilder builder, long rawValue) {
        builder.appendDouble(toDouble.convert(rawValue));
    }

    @Override
    protected Block tryDirectRead(OptionalColumnAtATimeReader direct, BlockFactory factory, Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        return direct.tryRead(factory, docs, offset, nullsFiltered, toDouble, false, false);
    }

    @Override
    public String toString() {
        return "DoublesFromDocValues[" + fieldName + "]";
    }
}
