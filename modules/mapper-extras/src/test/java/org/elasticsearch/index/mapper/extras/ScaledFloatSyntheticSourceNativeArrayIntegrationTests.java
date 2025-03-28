/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.index.mapper.NativeArrayIntegrationTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;

import static java.util.Collections.singletonList;

public class ScaledFloatSyntheticSourceNativeArrayIntegrationTests extends NativeArrayIntegrationTestCase {
    private static final double TEST_SCALING_FACTOR = 10.0;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return singletonList(MapperExtrasPlugin.class);
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "scaled_float").field("scaling_factor", TEST_SCALING_FACTOR);
    }

    @Override
    protected String getFieldTypeName() {
        fail("Should not be called because minimalMapping is overridden");
        return null;
    }

    @Override
    protected Object getRandomValue() {
        return randomLong() / TEST_SCALING_FACTOR;
    }

    @Override
    protected Object getMalformedValue() {
        return randomBoolean() ? RandomStrings.randomAsciiOfLength(random(), 8) : Double.POSITIVE_INFINITY;
    }
}
