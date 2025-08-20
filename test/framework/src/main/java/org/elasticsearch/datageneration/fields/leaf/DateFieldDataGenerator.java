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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

public class DateFieldDataGenerator implements FieldDataGenerator {
    private final DataSource dataSource;
    private final Supplier<Instant> instants;
    private final Supplier<String> strings;

    public DateFieldDataGenerator(DataSource dataSource) {
        this.dataSource = dataSource;
        this.instants = () -> dataSource.get(new DataSourceRequest.InstantGenerator()).generator().get();
        this.strings = dataSource.get(new DataSourceRequest.StringGenerator()).generator();
    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        Supplier<Object> supplier = () -> instants.get().toEpochMilli();

        if (fieldMapping != null && fieldMapping.get("format") != null) {
            String format = (String) fieldMapping.get("format");
            supplier = () -> DateTimeFormatter.ofPattern(format, Locale.ROOT).withZone(ZoneId.from(ZoneOffset.UTC)).format(instants.get());
        }

        if (fieldMapping != null && (Boolean) fieldMapping.getOrDefault("ignore_malformed", false)) {
            supplier = Wrappers.defaultsWithMalformed(supplier, strings::get, dataSource);
        } else {
            supplier = Wrappers.defaults(supplier, dataSource);
        }

        return supplier.get();
    }
}
