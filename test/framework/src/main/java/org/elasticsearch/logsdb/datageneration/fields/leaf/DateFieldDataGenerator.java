/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration.fields.leaf;

import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.datasource.DataSource;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;

import java.util.Map;
import java.util.function.Supplier;

public class DateFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> booleanGenerator;
    private final Supplier<Object> booleanWithBooleanStringsGenerator;
    private final Supplier<Object> valueGeneratorWithMalformed;

    public DateFieldDataGenerator(DataSource dataSource) {
        var booleans = dataSource.get(new DataSourceRequest.BooleanGenerator()).generator();
        this.booleanGenerator = Wrappers.defaults(booleans::get, dataSource);

        var transformation = dataSource.get(new DataSourceRequest.TransformWrapper(0.2, Object::toString));
        var booleansWithBooleanStrings = transformation.wrapper().apply(booleans::get);
        this.booleanWithBooleanStringsGenerator = Wrappers.defaults(booleansWithBooleanStrings, dataSource);

        var strings = dataSource.get(new DataSourceRequest.StringGenerator()).generator();
        this.valueGeneratorWithMalformed = Wrappers.defaultsWithMalformed(booleansWithBooleanStrings, strings::get, dataSource);
    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        if (fieldMapping == null) {
            // This is a dynamic field and returning string values like "true" will lead to mapping conflicts.
            return booleanGenerator.get();
        }

        if ((Boolean) fieldMapping.getOrDefault("ignore_malformed", false)) {
            return valueGeneratorWithMalformed.get();
        }

        return booleanWithBooleanStringsGenerator.get();
    }
}

