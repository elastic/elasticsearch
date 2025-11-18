/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.NumberFieldBlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class UnsignedLongFieldBlockLoaderTests extends NumberFieldBlockLoaderTestCase<Long> {
    private static final long MASK_2_63 = 0x8000000000000000L;

    public UnsignedLongFieldBlockLoaderTests(Params params) {
        super(FieldType.UNSIGNED_LONG, params);
    }

    @Override
    protected Long convert(Number value, Map<String, Object> fieldMapping) {
        // Adjust values coming from source to the way they are stored in doc_values.
        // See mapper implementation.
        var unsigned = value.longValue();
        return unsigned ^ MASK_2_63;
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new UnsignedLongMapperPlugin());
    }
}
