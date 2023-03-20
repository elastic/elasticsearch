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

    private final MappedFieldType fieldType;
    private final List<Object> values = new ArrayList<>();
    private boolean valuesLoaded = false;

    FieldLookup(MappedFieldType fieldType) {
        this.fieldType = fieldType;
    }

    MappedFieldType fieldType() {
        return fieldType;
    }

    /**
     * Sets the post processed values.
     */
    public void setValues(List<Object> values) {
        assert valuesLoaded == false : "Call clear() before calling setValues()";
        this.values.addAll(values);
        this.valuesLoaded = true;
    }

    public boolean isLoaded() {
        return valuesLoaded;
    }

    public void clear() {
        values.clear();
        valuesLoaded = false;
    }

    public List<Object> getValues() {
        return values;
    }

    public Object getValue() {
        return values.get(0);
    }
}
