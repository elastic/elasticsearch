/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.ArrayList;
import java.util.List;

public class FieldLookup {

    // we can cached fieldType completely per name, since its on an index/shard level (the lookup, and it does not change within the scope
    // of a search request)
    private final MappedFieldType fieldType;

    private final List<Object> values = new ArrayList<>();

    FieldLookup(MappedFieldType fieldType) {
        this.fieldType = fieldType;
    }

    MappedFieldType fieldType() {
        return fieldType;
    }

    public void addValue(Object value) {
        this.values.add(fieldType.valueForDisplay(value));
    }

    public void clear() {
        values.clear();
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public Object getValue() {
        return values.isEmpty() ? null : values.get(0);
    }

    public List<Object> getValues() {
        return values;
    }
}
