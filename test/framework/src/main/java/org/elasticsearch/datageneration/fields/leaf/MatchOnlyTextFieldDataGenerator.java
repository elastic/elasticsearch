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

import java.util.Map;

public class MatchOnlyTextFieldDataGenerator implements FieldDataGenerator {
    private final FieldDataGenerator textGenerator;

    public MatchOnlyTextFieldDataGenerator(DataSource dataSource) {
        this.textGenerator = new TextFieldDataGenerator(dataSource);
    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        return textGenerator.generateValue(fieldMapping);
    }
}
