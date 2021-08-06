/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

/**
 * An empty field is empty no matter the type, handles all the user-type coercion here.
 */
public class EmptyField implements Field<Number> {
    protected final String name;

    public EmptyField(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public List<Number> getValues() {
        return Collections.emptyList();
    }

    @Override
    public LongField asLongField() {
        return new LongField(name, EmptyFieldValues.LONG);
    }

    @Override
    public long asLong(long defaultValue) {
        return defaultValue;
    }

    @Override
    public DoubleField asDoubleField() {
        return new DoubleField(name, EmptyFieldValues.DOUBLE);
    }

    @Override
    public double asDouble(double defaultValue) {
        return defaultValue;
    }

    @Override
    public BigIntegerField asBigIntegerField() {
        return new BigIntegerField(name, EmptyFieldValues.BIGINTEGER);
    }

    @Override
    public BigInteger asBigInteger(BigInteger defaultValue) {
        return defaultValue;
    }

    @Override
    public StringField asStringField() {
        return new StringField(name, EmptyFieldValues.STRING);
    }

    @Override
    public String asString(String defaultValue) {
        return defaultValue;
    }

    @Override
    public ObjectField asObjectField() {
        return new ObjectField(name, EmptyFieldValues.OBJECT);
    }

    @Override
    public Object asObject(Object defaultValue) {
        return defaultValue;
    }
}
