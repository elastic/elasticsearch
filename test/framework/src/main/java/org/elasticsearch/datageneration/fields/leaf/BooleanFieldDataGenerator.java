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

public class BooleanFieldDataGenerator implements FieldDataGenerator {
    private final DataSource dataSource;
    private final Supplier<Object> booleans;
    private final Supplier<Object> booleansWithStrings;
    private final Supplier<Object> booleansWithStringsAndMalformed;

    public BooleanFieldDataGenerator(DataSource dataSource) {
        this.dataSource = dataSource;

        var booleans = dataSource.get(new DataSourceRequest.BooleanGenerator()).generator();
        this.booleans = booleans::get;

        // produces "true" and "false" strings
        var toStringTransform = dataSource.get(new DataSourceRequest.TransformWrapper(0.5, Object::toString)).wrapper();
        this.booleansWithStrings = toStringTransform.apply(this.booleans::get);

        var strings = dataSource.get(new DataSourceRequest.StringGenerator()).generator();
        this.booleansWithStringsAndMalformed = Wrappers.defaultsWithMalformed(booleansWithStrings, strings::get, dataSource);
    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        if (fieldMapping == null) {
            // dynamically mapped, use booleans only to avoid mapping the field as string
            return Wrappers.defaults(booleans, dataSource).get();
        }

        if ((Boolean) fieldMapping.getOrDefault("ignore_malformed", false)) {
            return booleansWithStringsAndMalformed.get();
        }

        return Wrappers.defaults(booleansWithStrings, dataSource).get();
    }
}
