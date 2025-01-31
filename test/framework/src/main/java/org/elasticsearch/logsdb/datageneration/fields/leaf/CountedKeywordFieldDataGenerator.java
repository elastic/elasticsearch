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
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public class CountedKeywordFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> valueGenerator;
    private final Set<String> previousStrings = new HashSet<>();

    public CountedKeywordFieldDataGenerator(String fieldName, DataSource dataSource) {
        var strings = dataSource.get(new DataSourceRequest.StringGenerator());
        var nulls = dataSource.get(new DataSourceRequest.NullWrapper());
        var arrays = dataSource.get(new DataSourceRequest.ArrayWrapper());

        this.valueGenerator = arrays.wrapper().compose(nulls.wrapper()).apply(() -> {
            if (previousStrings.size() > 0 && ESTestCase.randomBoolean()) {
                return ESTestCase.randomFrom(previousStrings);
            } else {
                String value = strings.generator().get();
                previousStrings.add(value);
                return value;
            }
        });
    }

    @Override
    public Object generateValue() {
        return valueGenerator.get();
    }
}
