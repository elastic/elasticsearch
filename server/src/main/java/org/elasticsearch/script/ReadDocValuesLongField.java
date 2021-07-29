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

public class ReadDocValuesLongField implements LongField {
    protected final String name;
    protected final ScriptDocValues.Longs scriptDocValues;

    public ReadDocValuesLongField(String name, ScriptDocValues.Longs scriptDocValues) {
        this.name = name;
        this.scriptDocValues = scriptDocValues;
    }

    @Override
    public long getValue(long defaultValue) {
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
    public List<Long> getValues() {
        return scriptDocValues;
    }
}
