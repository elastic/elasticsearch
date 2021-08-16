/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Collections;
import java.util.List;

/**
 * Script field with no mapping, always returns {@code defaultValue}.
 */
public class EmptyField<T> extends Field<T> {
    public EmptyField(String name) {
        super(name, null);
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public List<T> getValues() {
        return Collections.emptyList();
    }

    @Override
    public <CT, CF extends Field<CT>> Field<CT> as(Converter<CT, CF> converter) {
        // TODO(stu): test
        return converter.getFieldClass().cast(this);
    }

    @Override
    public T getValue(T defaultValue) {
        return defaultValue;
    }

    @Override
    public double getDouble(double defaultValue) {
        return defaultValue;
    }

    @Override
    public long getLong(long defaultValue) {
        return defaultValue;
    }
}
