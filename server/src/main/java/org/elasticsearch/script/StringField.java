/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.List;

public class StringField extends AbstractField<String, FieldValues.Strings> {
    public StringField(String name, FieldValues.Strings values) {
        super(name, values);
    }

    public String getValue(String defaultValue) {
        if (values.isEmpty()) {
            return defaultValue;
        }
        return values.getString(0);
    }

    @Override
    protected List<String> getFieldValues() {
        return values.getStrings();
    }

    @Override
    public StringField asStringField() {
        return this;
    }

    @Override
    public String asString(String defaultValue) {
        return getValue(defaultValue);
    }
}
