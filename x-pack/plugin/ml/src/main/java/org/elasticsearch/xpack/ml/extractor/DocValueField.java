/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchHit;

import java.util.Set;

public class DocValueField extends AbstractField {

    public DocValueField(String name, Set<String> types) {
        super(name, types);
    }

    @Override
    public Method getMethod() {
        return Method.DOC_VALUE;
    }

    @Override
    public Object[] value(SearchHit hit) {
        return getFieldValue(hit);
    }

    @Override
    public boolean supportsFromSource() {
        return true;
    }

    @Override
    public ExtractedField newFromSource() {
        return new SourceField(getSearchField(), getTypes());
    }

    @Override
    public boolean isMultiField() {
        return false;
    }

    @Nullable
    public String getDocValueFormat() {
        return null;
    }
}
