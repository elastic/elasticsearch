/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public class TestBlock implements BlockLoader.BooleanBuilder, BlockLoader.BytesRefBuilder, BlockLoader.DoubleBuilder {
    private final List<Object> values = new ArrayList<>();

    private List<Object> currentPosition = null;

    @Override
    public TestBlock appendNull() {
        assertNull(currentPosition);
        values.add(null);
        return this;
    }

    @Override
    public TestBlock beginPositionEntry() {
        assertNull(currentPosition);
        currentPosition = new ArrayList<>();
        values.add(currentPosition);
        return this;
    }

    @Override
    public TestBlock endPositionEntry() {
        assertNotNull(currentPosition);
        currentPosition = null;
        return this;
    }

    @Override
    public TestBlock appendBoolean(boolean value) {
        return add(value);
    }

    @Override
    public BlockLoader.BytesRefBuilder appendBytesRef(BytesRef value) {
        return add(BytesRef.deepCopyOf(value));
    }

    @Override
    public BlockLoader.DoubleBuilder appendDouble(double value) {
        return add(value);
    }

    private TestBlock add(Object value) {
        (currentPosition == null ? values : currentPosition).add(value);
        return this;
    }
}
