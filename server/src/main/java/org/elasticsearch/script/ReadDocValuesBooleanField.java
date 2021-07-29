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

public class ReadDocValuesBooleanField implements BooleanField {
    protected final String name;
    protected final ScriptDocValues.Booleans scriptDocValues;

    public ReadDocValuesBooleanField(String name, ScriptDocValues.Booleans scriptDocValues) {
        this.name = name;
        this.scriptDocValues = scriptDocValues;
    }

    @Override
    public boolean getValue(boolean defaultValue) {
        if (scriptDocValues.isEmpty()) {
            return defaultValue;
        }
        return scriptDocValues.get(0);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return scriptDocValues.isEmpty();
    }

    @Override
    public List<Boolean> getValues() {
        return scriptDocValues;
    }
}
