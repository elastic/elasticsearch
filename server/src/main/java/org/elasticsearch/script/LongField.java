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
 * Duck-typed {@code long} field.  Allows un-boxed access to the underlying {@code long} via
 * {@code getValue}.
 */
public class LongField extends AbstractField<Long, FieldValues.Longs> {
    public LongField(String name, FieldValues.Longs values) {
        super(name, values);
    }

    public long getValue(long defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        return values.getLong(0);
    }

    @Override
    protected List<Long> getFieldValues() {
        return values.getLongs();
    }

    @Override
    public LongField asLongField() {
        return this;
    }

    @Override
    public long asLong(long defaultValue) {
        return getValue(defaultValue);
    }
}
