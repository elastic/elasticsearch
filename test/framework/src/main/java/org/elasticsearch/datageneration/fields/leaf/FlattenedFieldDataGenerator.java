/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.fields.leaf;

import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.FieldDataGenerator;
import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class FlattenedFieldDataGenerator implements FieldDataGenerator {
    private final int maxObjectDepth;

    private final DataSourceResponse.ChildFieldGenerator fieldGenerator;
    private final Supplier<Object> longGenerator;
    private final Supplier<Object> doubleGenerator;
    private final Supplier<Object> stringGenerator;

    public FlattenedFieldDataGenerator(DataSource dataSource) {
        this(dataSource, 2, 6);
    }

    public FlattenedFieldDataGenerator(DataSource dataSource, int maxObjectDepth, int maxFieldCountPerLevel) {
        this.maxObjectDepth = maxObjectDepth;

        var spec = DataGeneratorSpecification.builder()
            .withMaxObjectDepth(maxObjectDepth)
            .withMaxFieldCountPerLevel(maxFieldCountPerLevel)
            .build();

        this.fieldGenerator = dataSource.get(new DataSourceRequest.ChildFieldGenerator(spec));

        var nulls = dataSource.get(new DataSourceRequest.NullWrapper()).wrapper();
        var arrays = dataSource.get(new DataSourceRequest.ArrayWrapper()).wrapper();
        var repeats = dataSource.get(new DataSourceRequest.RepeatingWrapper()).wrapper();
        var composed = nulls.compose(arrays.compose(repeats));

        var longs = dataSource.get(new DataSourceRequest.LongGenerator()).generator();
        var doubles = dataSource.get(new DataSourceRequest.DoubleGenerator()).generator();
        var strings = dataSource.get(new DataSourceRequest.StringGenerator()).generator();

        this.longGenerator = composed.apply(longs::get);
        this.doubleGenerator = composed.apply(doubles::get);
        this.stringGenerator = composed.apply(strings::get);
    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        return generateObject(0);
    }

    private Map<String, Object> generateObject(int depth) {
        Map<String, Object> value = new HashMap<>();
        int fieldCount = fieldGenerator.generateChildFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (depth < maxObjectDepth && fieldGenerator.generateRegularSubObject()) {
                value.put(generateFieldName(), generateObject(depth + 1));
            } else {
                value.put(generateFieldName(), randomLeafValue());
            }
        }
        return value;
    }

    private String generateFieldName() {
        while (true) {
            var candidate = fieldGenerator.generateFieldName();
            if (candidate.contains("\0")) {
                continue;
            }
            return candidate;
        }
    }

    private Object randomLeafValue() {
        return switch (ESTestCase.randomInt(2)) {
            case 0 -> longGenerator.get();
            case 1 -> doubleGenerator.get();
            case 2 -> stringGenerator.get();
            default -> throw new IllegalStateException();
        };
    }
}
