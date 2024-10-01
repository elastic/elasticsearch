/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class DefaultObjectGenerationHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
        return new DataSourceResponse.ChildFieldGenerator() {
            @Override
            public int generateChildFieldCount() {
                return ESTestCase.randomIntBetween(0, request.specification().maxFieldCountPerLevel());
            }

            @Override
            public boolean generateDynamicSubObject() {
                // Using a static 5% change, this is just a chosen value that can be tweaked.
                return randomDouble() <= 0.05;
            }

            @Override
            public boolean generateNestedSubObject() {
                // Using a static 5% change, this is just a chosen value that can be tweaked.
                return randomDouble() <= 0.05;
            }

            @Override
            public boolean generateRegularSubObject() {
                // Using a static 5% change, this is just a chosen value that can be tweaked.
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
        Supplier<DataSourceResponse.FieldTypeGenerator.FieldTypeInfo> generator = switch (request.dynamicMapping()) {
            case FORBIDDEN -> () -> generateFieldTypeInfo(false);
            case FORCED -> () -> generateFieldTypeInfo(true);
            case SUPPORTED -> () -> generateFieldTypeInfo(ESTestCase.randomBoolean());
        };

        return new DataSourceResponse.FieldTypeGenerator(generator);
    }

    private static DataSourceResponse.FieldTypeGenerator.FieldTypeInfo generateFieldTypeInfo(boolean isDynamic) {
        var excluded = isDynamic ? EXCLUDED_FROM_DYNAMIC_MAPPING : Set.of();

        var fieldType = ESTestCase.randomValueOtherThanMany(excluded::contains, () -> ESTestCase.randomFrom(FieldType.values()));

        return new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(fieldType, isDynamic);
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
}
