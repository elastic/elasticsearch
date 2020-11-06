/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

import java.util.List;
import java.util.Objects;
import java.util.Set;

abstract class AbstractField implements ExtractedField {

    private final String name;

    private final Set<String> types;

    AbstractField(String name, Set<String> types) {
        this.name = Objects.requireNonNull(name);
        this.types = Objects.requireNonNull(types);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getSearchField() {
        return name;
    }

    @Override
    public Set<String> getTypes() {
        return types;
    }

    protected Object[] getFieldValue(SearchHit hit) {
        DocumentField keyValue = hit.field(getSearchField());
        if (keyValue != null) {
            List<Object> values = keyValue.getValues();
            return values.toArray(new Object[0]);
        }
        return new Object[0];
    }
}
