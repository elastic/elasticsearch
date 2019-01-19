/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.sql.analysis.index.MappingException;

import java.util.Map;

/**
 * SQL-related information about an index field with text type
 */
public class TextEsField extends EsField {

    public TextEsField(String name, Map<String, EsField> properties, boolean hasDocValues) {
        super(name, DataType.TEXT, properties, hasDocValues);
    }

    @Override
    public EsField getExactField() {
        EsField field = null;
        for (EsField property : getProperties().values()) {
            if (property.getDataType() == DataType.KEYWORD && property.isExact()) {
                if (field != null) {
                    throw new MappingException("Multiple exact keyword candidates available for [" + getName() +
                            "]; specify which one to use");
                }
                field = property;
            }
        }
        if (field == null) {
            throw new MappingException("No keyword/multi-field defined exact matches for [" + getName() +
                    "]; define one or use MATCH/QUERY instead");
        }
        return field;
    }

    @Override
    public boolean isExact() {
        return false;
    }
}
