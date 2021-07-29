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

public class ReadDocValuesDoubleField implements DoubleField {
    protected final String name;
    protected final ScriptDocValues.Doubles scriptDocValues;

    public ReadDocValuesDoubleField(String name, ScriptDocValues.Doubles scriptDocValues) {
        this.name = name;
        this.scriptDocValues = scriptDocValues;
    }

    @Override
    public double getValue(double defaultValue) {
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
    public List<Double> getValues() {
        return scriptDocValues;
    }
}
