/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;

import java.util.Objects;
import java.util.Set;

public class MultiField implements ExtractedField {

    private final String name;
    private final String searchField;
    private final ExtractedField field;
    private final String parent;

    public MultiField(String parent, ExtractedField field) {
        this(field.getName(), field.getSearchField(), parent, field);
    }

    MultiField(String name, String searchField, String parent, ExtractedField field) {
        this.name = Objects.requireNonNull(name);
        this.searchField = Objects.requireNonNull(searchField);
        this.field = Objects.requireNonNull(field);
        this.parent = Objects.requireNonNull(parent);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getSearchField() {
        return searchField;
    }

    @Override
    public Set<String> getTypes() {
        return field.getTypes();
    }

    @Override
    public Method getMethod() {
        return field.getMethod();
    }

    @Override
    public Object[] value(SearchHit hit) {
        return field.value(hit);
    }

    @Override
    public boolean supportsFromSource() {
        return false;
    }

    @Override
    public ExtractedField newFromSource() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMultiField() {
        return true;
    }

    @Override
    public String getParentField() {
        return parent;
    }

    @Override
    public String getDocValueFormat() {
        return field.getDocValueFormat();
    }
}
