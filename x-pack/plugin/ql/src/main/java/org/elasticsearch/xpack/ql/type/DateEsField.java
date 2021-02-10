/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.type;

import java.util.Map;

/**
 * SQL-related information about an index field with date type
 */
public class DateEsField extends EsField {

    public static DateEsField dateEsField(String name, Map<String, EsField> properties, boolean hasDocValues) {
        return new DateEsField(name, DataTypes.DATETIME, properties, hasDocValues);
    }

    private DateEsField(String name, DataType dataType, Map<String, EsField> properties, boolean hasDocValues) {
        super(name, dataType, properties, hasDocValues);
    }
}
