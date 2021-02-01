/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

    public static DateEsField dateNanosEsField(String name, Map<String, EsField> properties, boolean hasDocValues) {
        return new DateEsField(name, DataTypes.DATETIME_NANOS, properties, hasDocValues);
    }

    private DateEsField(String name, DataType dataType, Map<String, EsField> properties, boolean hasDocValues) {
        super(name, dataType, properties, hasDocValues);
    }
}
