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

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomRealisticUnicodeOfCodepointLengthBetween;

public class DefaultObjectGenerationHandler implements DataSourceHandler {

    /**
     * Field names will not be generated which start with `_reserved_`. Handlers can safely
     * create field names starting with this prefix without the concern of randomly generated
     * fields having the same name.
     */
    public static final String RESERVED_FIELD_NAME_PREFIX = "_reserved_";

    public static class DefaultChildFieldGenerator implements DataSourceResponse.ChildFieldGenerator {
        private final DataSourceRequest.ChildFieldGenerator request;

        public DefaultChildFieldGenerator(DataSourceRequest.ChildFieldGenerator request) {
            this.request = request;
        }

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
            while (true) {
                String fieldName = randomRealisticUnicodeOfCodepointLengthBetween(1, 10);
                if (fieldName.isBlank()) {
                    continue;
                }
                if (fieldName.indexOf('.') != -1) {
                    continue;
                }
                if (fieldName.startsWith(RESERVED_FIELD_NAME_PREFIX)) {
                    continue;
                }

                return fieldName;
            }
        }
    }

    @Override
    public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
        return new DefaultChildFieldGenerator(request);
    }

    // UNSIGNED_LONG is excluded because it is mapped as long
    // and values larger than long fail to parse.
    private static final Set<FieldType> EXCLUDED_FROM_DYNAMIC_MAPPING = Set.of(FieldType.UNSIGNED_LONG, FieldType.PASSTHROUGH);
    private static final Set<FieldType> ALLOWED_FIELD_TYPES = Arrays.stream(FieldType.values())
        .filter(fieldType -> EXCLUDED_FROM_DYNAMIC_MAPPING.contains(fieldType) == false)
        .collect(Collectors.toSet());

    @Override
    public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
        return new DataSourceResponse.FieldTypeGenerator(() -> {
            var fieldType = ESTestCase.randomFrom(ALLOWED_FIELD_TYPES);
            return new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(fieldType.toString());
        });
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
