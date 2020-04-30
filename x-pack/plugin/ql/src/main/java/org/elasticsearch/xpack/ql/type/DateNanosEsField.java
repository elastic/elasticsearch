/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.type;

import java.util.Map;

/**
 * SQL-related information about an index field with date_nanos type
 */
public class DateNanosEsField extends EsField {

    public DateNanosEsField(String name, Map<String, EsField> properties, boolean hasDocValues) {
        super(name, DataTypes.DATETIME_NANOS, properties, hasDocValues);
    }
}
