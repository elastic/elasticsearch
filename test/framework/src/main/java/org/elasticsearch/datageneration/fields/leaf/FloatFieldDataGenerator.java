/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.fields.leaf;

import org.elasticsearch.datageneration.FieldDataGenerator;
import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;

import java.util.Map;
import java.util.function.Supplier;

public class FloatFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> valueGenerator;
    private final Supplier<Object> valueGeneratorWithMalformed;

    public FloatFieldDataGenerator(String fieldName, DataSource dataSource) {
        var floats = dataSource.get(new DataSourceRequest.FloatGenerator()).generator();

        this.valueGenerator = Wrappers.defaults(floats::get, dataSource);

        var strings = dataSource.get(new DataSourceRequest.StringGenerator()).generator();
        this.valueGeneratorWithMalformed = Wrappers.defaultsWithMalformed(floats::get, strings::get, dataSource);
    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        if (fieldMapping != null && (Boolean) fieldMapping.getOrDefault("ignore_malformed", false)) {
            return valueGeneratorWithMalformed.get();
        }

        return valueGenerator.get();
    }
}
