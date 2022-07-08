/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedField;

/**
 * Used by all field data based aggregators. This determine the context of the field data the aggregators are operating
 * in. It holds both the field names and the index field datas that are associated with them.
 */
public class FieldContext {

    private final String field;
    private final IndexFieldData<?> indexFieldData;
    private final MappedField mappedField;

    /**
     * Constructs a field data context for the given field and its index field data
     *
     * @param field             The name of the field
     * @param indexFieldData    The index field data of the field
     */
    public FieldContext(String field, IndexFieldData<?> indexFieldData, MappedField mappedField) {
        this.field = field;
        this.indexFieldData = indexFieldData;
        this.mappedField = mappedField;
    }

    public String field() {
        return field;
    }

    /**
     * @return The index field datas in this context
     */
    public IndexFieldData<?> indexFieldData() {
        return indexFieldData;
    }

    public MappedField mappedField() {
        return mappedField;
    }

    public String getTypeName() {
        return mappedField.typeName();
    }

}
