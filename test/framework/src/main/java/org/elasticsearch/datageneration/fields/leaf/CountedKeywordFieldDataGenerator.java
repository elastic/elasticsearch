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

public class CountedKeywordFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> valueGenerator;

    public CountedKeywordFieldDataGenerator(String fieldName, DataSource dataSource) {
        var strings = dataSource.get(new DataSourceRequest.StringGenerator());
        var nulls = dataSource.get(new DataSourceRequest.NullWrapper());
        var arrays = dataSource.get(new DataSourceRequest.ArrayWrapper());
        var repeats = dataSource.get(new DataSourceRequest.RepeatingWrapper());

        this.valueGenerator = arrays.wrapper().compose(nulls.wrapper().compose(repeats.wrapper())).apply(() -> strings.generator().get());
    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        return valueGenerator.get();
    }
}
