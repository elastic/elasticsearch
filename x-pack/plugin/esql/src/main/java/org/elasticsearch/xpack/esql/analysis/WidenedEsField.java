/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.Map;

public class WidenedEsField extends EsField {
    private final DataType originalDataType;

    public WidenedEsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean aggregatable,
        boolean isAlias,
        DataType originalDataType
    ) {
        super(name, esDataType, properties, aggregatable, isAlias);
        this.originalDataType = originalDataType;
    }

    public DataType getOriginalDataType() {
        return originalDataType;
    }
}
