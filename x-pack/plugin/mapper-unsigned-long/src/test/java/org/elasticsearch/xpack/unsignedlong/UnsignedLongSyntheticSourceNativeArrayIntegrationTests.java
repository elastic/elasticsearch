/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.index.mapper.NativeArrayIntegrationTestCase;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

public class UnsignedLongSyntheticSourceNativeArrayIntegrationTests extends NativeArrayIntegrationTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(UnsignedLongMapperPlugin.class);
    }

    @Override
    protected String getFieldTypeName() {
        return "unsigned_long";
    }

    @Override
    protected Long getRandomValue() {
        return randomNonNegativeLong();
    }

    @Override
    protected String getMalformedValue() {
        return RandomStrings.randomAsciiOfLength(random(), 8);
    }
}
