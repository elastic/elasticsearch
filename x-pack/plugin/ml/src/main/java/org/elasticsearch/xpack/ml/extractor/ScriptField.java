/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;

import java.util.Collections;

public class ScriptField extends AbstractField {

    public ScriptField(String name) {
        super(name, Collections.emptySet());
    }

    @Override
    public Method getMethod() {
        return Method.SCRIPT_FIELD;
    }

    @Override
    public Object[] value(SearchHit hit, SourceSupplier source) {
        return getFieldValue(hit);
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
        return false;
    }
}
