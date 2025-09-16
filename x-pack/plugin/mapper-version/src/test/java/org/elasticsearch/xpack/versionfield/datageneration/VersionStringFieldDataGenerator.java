/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield.datageneration;

import org.elasticsearch.datageneration.FieldDataGenerator;
import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;

import java.util.Map;
import java.util.function.Supplier;

public class VersionStringFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> values;

    public VersionStringFieldDataGenerator(DataSource dataSource) {
        var nullWrapper = dataSource.get(new DataSourceRequest.NullWrapper());
        var versionStrings = dataSource.get(new DataSourceRequest.VersionStringGenerator());

        this.values = nullWrapper.wrapper().apply(versionStrings.generator()::get);
    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        return this.values.get();
    }
}
