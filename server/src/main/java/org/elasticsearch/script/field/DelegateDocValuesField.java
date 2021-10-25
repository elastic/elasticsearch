/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;
import java.util.List;

/**
 * A default {@link Field} to provide {@code ScriptDocValues} for fields
 * that are not supported by the script fields api.
 */
public class DelegateDocValuesField implements DocValuesField {

    private final ScriptDocValues<?> scriptDocValues;
    private final String name;

    public DelegateDocValuesField(ScriptDocValues<?> scriptDocValues, String name) {
        this.scriptDocValues = scriptDocValues;
        this.name = name;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        scriptDocValues.setNextDocId(docId);
    }

    @Override
    public ScriptDocValues<?> getScriptDocValues() {
        return scriptDocValues;
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException("field [" + name + "] is not supported through the fields api, use [doc] instead");
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("field [" + name + "] is not supported through the fields api, use [doc] instead");
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("field [" + name + "] is not supported through the fields api, use [doc] instead");
    }

    public Object getValue(Object defaultValue) {
        throw new UnsupportedOperationException("field [" + name + "] is not supported through the fields api, use [doc] instead");
    }

    public List<?> getValues() {
        throw new UnsupportedOperationException("field [" + name + "] is not supported through the fields api, use [doc] instead");
    }
}
