/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.index.mapper.OffsetDocValuesLoaderTestCase;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

public class UnsignedLongOffsetDocValuesLoaderTests extends OffsetDocValuesLoaderTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new UnsignedLongMapperPlugin());
    }

    @Override
    public String getFieldTypeName() {
        return "unsigned_long";
    }

    @Override
    public Object randomValue() {
        return randomNonNegativeLong();
    }
}
