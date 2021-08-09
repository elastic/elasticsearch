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
 * Fields API for a Objects that don't yet have specialized {@code getValue}s implemented yet.
 */
public class DefField extends AbstractField<Object, FieldValues.Objects> {
    public DefField(String name, FieldValues.Objects values) {
        super(name, values);
    }

    public Object getValue(Object defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        return values.getObject(0);
    }

    @Override
    protected List<Object> getFieldValues() {
        return values.getObjects();
    }

    @Override
    public DefField asDefField() {
        return this;
    }

    @Override
    public Object asDef(Object defaultValue) {
        return getValue(defaultValue);
    }
}
