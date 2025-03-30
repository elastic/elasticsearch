/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.index.mapper.OffsetDocValuesLoaderTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;

import static java.util.Collections.singletonList;

public class ScaledFloatOffsetDocValuesLoaderTests extends OffsetDocValuesLoaderTestCase {
    private static final double TEST_SCALING_FACTOR = 10.0;

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new MapperExtrasPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "scaled_float").field("scaling_factor", TEST_SCALING_FACTOR);
    }

    public void testOffsetArray() throws Exception {
        verifyOffsets("{\"field\":[1.0,10.0,100.0,0.1,10.0,1.0,0.1,100.0]}");
        verifyOffsets("{\"field\":[10.0,null,1.0,null,5.0,null,null,6.3,1.5]}");
    }

    @Override
    protected String getFieldTypeName() {
        fail("Should not be called because minimalMapping is overridden");
        return null;
    }

    @Override
    protected Double randomValue() {
        return randomLong() / TEST_SCALING_FACTOR;
    }
}
