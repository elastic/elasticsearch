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
import java.util.Map;

public class FieldLookup {

    // we can cached fieldType completely per name, since its on an index/shard level (the lookup, and it does not change within the scope
    // of a search request)
    private final MappedFieldType fieldType;

    private Map<String, List<Object>> fields;

    private Object value;

    private boolean valueLoaded = false;

    private List<Object> values = new ArrayList<>();

    private boolean valuesLoaded = false;

    FieldLookup(MappedFieldType fieldType) {
        this.fieldType = fieldType;
    }

    MappedFieldType fieldType() {
        return fieldType;
    }

    public Map<String, List<Object>> fields() {
        return fields;
    }

    /**
     * Sets the post processed values.
     */
    public void fields(Map<String, List<Object>> fields) {
        this.fields = fields;
    }

    public void clear() {
        value = null;
        valueLoaded = false;
        values.clear();
        valuesLoaded = false;
        fields = null;
    }

    public boolean isEmpty() {
        if (valueLoaded) {
            return value == null;
        }
        if (valuesLoaded) {
            return values.isEmpty();
        }
        return getValue() == null;
    }

    public Object getValue() {
        if (valueLoaded) {
            return value;
        }
        valueLoaded = true;
        value = null;
        List<Object> values = fields.get(fieldType.name());
        return values != null ? value = values.get(0) : null;
    }

    public List<Object> getValues() {
        if (valuesLoaded) {
            return values;
        }
        valuesLoaded = true;
        values.clear();
        return values = fields().get(fieldType.name());
    }
}
