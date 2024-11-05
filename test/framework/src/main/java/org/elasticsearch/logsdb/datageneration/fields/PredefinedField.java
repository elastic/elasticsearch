/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.logsdb.datageneration.datasource.DataSource;

import java.util.Map;

public interface PredefinedField {
    String name();

    FieldType fieldType();

    Map<String, Object> mapping();

    FieldDataGenerator generator(DataSource dataSource);

    record WithGenerator(String name, FieldType fieldType, Map<String, Object> mapping, FieldDataGenerator generator)
        implements
            PredefinedField {
        @Override
        public FieldDataGenerator generator(DataSource dataSource) {
            return generator;
        }
    }
}
