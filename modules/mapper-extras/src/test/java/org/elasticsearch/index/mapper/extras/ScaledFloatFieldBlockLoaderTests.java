/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.NumberFieldBlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ScaledFloatFieldBlockLoaderTests extends NumberFieldBlockLoaderTestCase<Double> {
    public ScaledFloatFieldBlockLoaderTests(Params params) {
        super(FieldType.SCALED_FLOAT, params);
    }

    @Override
    protected Double convert(Number value, Map<String, Object> fieldMapping) {
        var scalingFactor = ((Number) fieldMapping.get("scaling_factor")).doubleValue();

        // Adjust values coming from source to the way they are stored in doc_values.
        // See mapper implementation.
        return Math.round(value.doubleValue() * scalingFactor) / scalingFactor;
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }
}
