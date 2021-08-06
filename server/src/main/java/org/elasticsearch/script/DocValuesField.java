/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.util.List;

/**
 * Fields API accessor for DocValues, users must currently call their own casting methods using
 * {@code as<T>Field}.  This is the initial entry point to that casting system.  Could be replaced
 * with field type inspection via {@code SearchLookup} in the future.
 */
public class DocValuesField<T> extends AbstractField<T, ScriptDocValues<T>> {
    protected final ScriptDocValues<T> scriptDocValues;

    public DocValuesField(String name, ScriptDocValues<T> scriptDocValues) {
        super(name, scriptDocValues);
        this.scriptDocValues = scriptDocValues;
    }

    @Override
    public List<T> getValues() {
        return scriptDocValues;
    }

    @Override
    protected List<T> getFieldValues() {
        return scriptDocValues;
    }
}
