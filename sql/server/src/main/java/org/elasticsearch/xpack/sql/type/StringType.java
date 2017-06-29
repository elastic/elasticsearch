/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.util.Map;

import static java.util.Collections.emptyMap;

import static org.elasticsearch.xpack.sql.util.ObjectUtils.mapCollector;

public abstract class StringType implements DataType {

    private final boolean docValue;
    private final Map<String, DataType> fields;
    private final Map<String, DataType> docValueFields;

    StringType(boolean docValue, Map<String, DataType> fields) {
        this.docValue = docValue;
        this.fields = fields;

        if (docValue || fields.isEmpty()) {
            docValueFields = emptyMap();
        }
        else {
            docValueFields = fields.entrySet().stream()
                    .filter(e -> e.getValue().hasDocValues())
                    .collect(mapCollector());
        }
    }

    @Override
    public JDBCType sqlType() {
        return JDBCType.VARCHAR;
    }

    @Override
    public boolean hasDocValues() {
        return docValue;
    }

    public Map<String, DataType> fields() {
        return fields;
    }

    public Map<String, DataType> docValueFields() {
        return docValueFields;
    }

    @Override
    public boolean isPrimitive() {
        return fields.isEmpty();
    }

    @Override
    public String toString() {
        return esName();
    }
}