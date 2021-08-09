/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.List;

/**
 * Duck-typed {@code double} field.  Allows un-boxed access to the underlying {@code double} via
 * {@code getValue}.
 */
public class DoubleField extends AbstractField<Double, FieldValues.Doubles> {
    public DoubleField(String name, FieldValues.Doubles values) {
        super(name, values);
    }

    @Override
    public Object getValue(Object defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        return values.getDouble(0);
    }

    @Override
    public double getDouble(double defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        return values.getDouble(0);
    }

    @Override
    protected List<Double> getFieldValues() {
        return values.getDoubles();
    }

    @Override
    public DoubleField asDoubleField() {
        return this;
    }
}
