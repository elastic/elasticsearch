/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.xpack.sql.analysis.catalog.MappingException;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.TextType;

public abstract class FieldAttribute extends TypedAttribute {

    FieldAttribute(Location location, String name, DataType dataType) {
        this(location, name, dataType, null, true, null, false);
    }

    FieldAttribute(Location location, String name, DataType dataType, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        super(location, name, dataType, qualifier, nullable, id, synthetic);
    }

    public boolean isAnalyzed() {
        return dataType() instanceof TextType;
    }

    public FieldAttribute notAnalyzedAttribute() {
        if (isAnalyzed()) {
            Map<String, DataType> docValueFields = ((TextType) dataType()).docValueFields();
            if (docValueFields.size() == 1) {
                Entry<String, DataType> entry = docValueFields.entrySet().iterator().next();
                return with(entry.getKey(), entry.getValue());
            }
            if (docValueFields.isEmpty()) {
                throw new MappingException("No docValue multi-field defined for %s", name());
            }
            if (docValueFields.size() > 1) {
                DataType dataType = docValueFields.get("keyword");
                if (dataType != null && dataType.hasDocValues()) {
                    return with("keyword", dataType);
                }
                throw new MappingException("Default 'keyword' not available as multi-fields and multiple options available for %s", name());
            }
        }
        return this;
    }

    protected FieldAttribute with(String subFieldName, DataType type) {
        return (FieldAttribute) clone(location(), name() + "." + subFieldName, type, qualifier(), nullable(), id(), synthetic());
    }
}
