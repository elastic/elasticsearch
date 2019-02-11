/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.util.Map;

/**
 * SQL-related information about an index field with date type
 */
public class DateEsField extends EsField {

    public DateEsField(String name, Map<String, EsField> properties, boolean hasDocValues) {
        super(name, DataType.DATETIME, properties, hasDocValues);
    }
}
