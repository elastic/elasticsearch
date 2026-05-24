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

public class BooleansBlockLoader extends AbstractNumericBlockLoader<BlockLoader.BooleanBuilder> {

    public BooleansBlockLoader(String fieldName) {
        this(fieldName, false);
    }

    public BooleansBlockLoader(String fieldName, boolean readInArrayOrder) {
        super(fieldName, "BooleansFromDocValues", readInArrayOrder);
    }

    @Override
    public BooleanBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.booleans(expectedCount);
    }

    @Override
    protected BooleanBuilder newBuilder(BlockFactory factory, int expectedCount) {
        return factory.booleansFromDocValues(expectedCount);
    }

    @Override
    protected void appendValue(BooleanBuilder builder, long rawValue) {
        builder.appendBoolean(rawValue != 0);
    }

    @Override
    public String toString() {
        return "BooleansFromDocValues[" + fieldName + "]";
    }
}
