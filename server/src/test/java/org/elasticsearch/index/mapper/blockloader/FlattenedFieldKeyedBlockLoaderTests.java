/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.datageneration.datasource.DefaultObjectGenerationHandler;
import org.elasticsearch.index.mapper.BinaryDVBlockLoaderTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

public class FlattenedFieldKeyedBlockLoaderTests extends BinaryDVBlockLoaderTestCase {

    private static final DataSourceHandler FLATTENED_DATA_GENERATOR = new DataSourceHandler() {
        @Override
        public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
            return new DefaultObjectGenerationHandler.DefaultChildFieldGenerator(request) {
                @Override
                public int generateChildFieldCount() {
                    // guarantee always at least 1 child field
                    return ESTestCase.randomIntBetween(1, request.specification().maxFieldCountPerLevel());
                }
            };
        }

        @Override
        public DataSourceResponse.ArrayWrapper handle(DataSourceRequest.ArrayWrapper request) {
            return new DataSourceResponse.ArrayWrapper(values -> () -> {
                if (ESTestCase.randomBoolean()) {
                    // Prevent empty arrays
                    var size = ESTestCase.randomIntBetween(1, 5);
                    return IntStream.range(0, size).mapToObj((i) -> values.get()).toList();
                }

                return values.get();
            });
        }

        @Override
        public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
            return new DataSourceResponse.ObjectArrayGenerator(() -> {
                if (ESTestCase.randomBoolean()) {
                    // Prevent empty arrays
                    return Optional.of(randomIntBetween(1, 5));
                }

                return Optional.empty();
            });
        }
    };

    public FlattenedFieldKeyedBlockLoaderTests(Params params) {
        super("flattened", List.of(FLATTENED_DATA_GENERATOR), params);
    }

    @SuppressWarnings("unchecked")
    private void chooseRandomSubfield(Object value, StringBuilder path) {
        if (value instanceof List<?> listValue) {
            Object nextValue = randomFrom(listValue);
            chooseRandomSubfield(nextValue, path);
        } else if (value instanceof Map) {
            var mapValue = (Map<String, Object>) value;
            String nextKey = randomFrom(mapValue.keySet());
            path.append(".");
            path.append(nextKey);
            Object nextValue = mapValue.get(nextKey);
            chooseRandomSubfield(nextValue, path);
        } else {
            assert value instanceof String || value instanceof Long || value instanceof Double || value == null;
        }
    }

    @Override
    protected String getFieldNameToLoad(String fieldName, Object value) {
        StringBuilder path = new StringBuilder();
        chooseRandomSubfield(value, path);

        assert path.isEmpty() == false;

        return fieldName + path;
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        var nullValue = (String) fieldMapping.get("null_value");
        if (value == null) {
            return convert(null, nullValue, Integer.MAX_VALUE);
        }

        boolean hasDocValues = hasDocValues(fieldMapping, true);
        int ignoreAbove = fieldMapping.get("ignore_above") != null && hasDocValues
            ? ((Number) fieldMapping.get("ignore_above")).intValue()
            : Integer.MAX_VALUE;

        if (value instanceof List<?> valueList) {
            var valueStream = valueList.stream().map(v -> convert(v, nullValue, ignoreAbove)).filter(Objects::nonNull);
            if (hasDocValues) {
                valueStream = valueStream.distinct().sorted();
            }
            return maybeFoldList(valueStream.toList());
        }

        return convert(value, nullValue, ignoreAbove);
    }

    private static BytesRef convert(Object value, String nullValue, int ignoreAbove) {
        if (value == null) {
            if (nullValue != null) {
                value = nullValue;
            } else {
                return null;
            }
        }

        String valueStr = value.toString();

        return valueStr.length() <= ignoreAbove ? new BytesRef(valueStr) : null;
    }

    @Override
    protected boolean supportsMultiField() {
        return false;
    }
}
