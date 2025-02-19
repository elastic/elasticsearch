/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.index.mapper.NumberFieldBlockLoaderTestCase;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ScaledFloatFieldBlockLoaderTests extends NumberFieldBlockLoaderTestCase<Double> {
    public ScaledFloatFieldBlockLoaderTests() {
        super(FieldType.SCALED_FLOAT);
    }

    @Override
    protected Double convert(Number value, Map<String, Object> fieldMapping) {
        var scalingFactor = ((Number) fieldMapping.get("scaling_factor")).doubleValue();

        var docValues = (boolean) fieldMapping.getOrDefault("doc_values", false);

        // There is a slight inconsistency between values that are read from doc_values and from source.
        // Due to how precision reduction is applied to source values so that they are consistent with doc_values.
        // See #122547.
        if (docValues) {
            var reverseScalingFactor = 1d / scalingFactor;
            return Math.round(value.doubleValue() * scalingFactor) * reverseScalingFactor;
        }

        // Adjust values coming from source to the way they are stored in doc_values.
        // See mapper implementation.
        return Math.round(value.doubleValue() * scalingFactor) / scalingFactor;
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }
}
