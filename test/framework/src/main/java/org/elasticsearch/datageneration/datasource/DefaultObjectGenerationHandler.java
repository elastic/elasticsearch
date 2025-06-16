/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.datasource;

import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class DefaultObjectGenerationHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
        return new DataSourceResponse.ChildFieldGenerator() {
            @Override
            public int generateChildFieldCount() {
                // no child fields is legal
                return ESTestCase.randomIntBetween(0, request.specification().maxFieldCountPerLevel());
            }

            @Override
            public boolean generateDynamicSubObject() {
                // Using a static 5% chance, this is just a chosen value that can be tweaked.
                return randomDouble() <= 0.05;
            }

            @Override
            public boolean generateNestedSubObject() {
                // Using a static 5% chance, this is just a chosen value that can be tweaked.
                return randomDouble() <= 0.05;
            }

            @Override
            public boolean generateRegularSubObject() {
                // Using a static 5% chance, this is just a chosen value that can be tweaked.
                return randomDouble() <= 0.05;
            }

            @Override
            public String generateFieldName() {
                return randomAlphaOfLengthBetween(1, 10);
            }
        };
    }

    // UNSIGNED_LONG is excluded because it is mapped as long
    // and values larger than long fail to parse.
    private static final Set<FieldType> EXCLUDED_FROM_DYNAMIC_MAPPING = Set.of(FieldType.UNSIGNED_LONG);

    @Override
    public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
        return new DataSourceResponse.FieldTypeGenerator(
            () -> new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(ESTestCase.randomFrom(FieldType.values()).toString())
        );
    }

    @Override
    public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
        return new DataSourceResponse.ObjectArrayGenerator(() -> {
            if (ESTestCase.randomBoolean()) {
                return Optional.of(randomIntBetween(0, 5));
            }

            return Optional.empty();
        });
    }

    @Override
    public DataSourceResponse.DynamicMappingGenerator handle(DataSourceRequest.DynamicMappingGenerator request) {
        // Using a static 5% chance for objects, this is just a chosen value that can be tweaked.
        return new DataSourceResponse.DynamicMappingGenerator(
            isObject -> isObject ? ESTestCase.randomDouble() <= 0.05 : ESTestCase.randomBoolean()
        );
    }
}
