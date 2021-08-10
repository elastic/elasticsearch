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
 * Parent of all Field implementations, handles common actions such as the {@code as<T>Field}, {@code as<T>}, {@code getName()}
 * API calls.  Subclasses should override {@code getFieldValues} for {@code getValues()}, they should also override their
 * own {@code as<T>Field}, {@code as<T>} to avoid new object creation.
 *
 * Of course, for duck typing, subclasses must also implement their own {@code getValue}.
 *
 * @param <T> Type of value held by this field.  Boxed for primitive types.
 * @param <V> Field values subclass used to implement common methods for Field.
 */
public abstract class AbstractField<T, V extends FieldValues> implements Field<T> {
    protected final String name;
    protected final V values;

    public AbstractField(String name, V values) {
        this.name = name;
        this.values = values;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<T> getValues() {
        if (isEmpty()) {
            return Collections.emptyList();
        }
        return getFieldValues();
    }

    protected abstract List<T> getFieldValues();

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public LongField asLongField() {
        if (values instanceof FieldValues.Longs == false) {
            throw new IllegalStateException("This Field cannot be converted to a LongField due to the underlying data [" +
                    values.getClass().getSimpleName() + "]");
        }

        return new LongField(name, (FieldValues.Longs) values);
    }

    @Override
    public long getLong(long defaultValue) {
        return asLongField().getLong(defaultValue);
    }

    @Override
    public DoubleField asDoubleField() {
        if (values instanceof FieldValues.Doubles == false) {
            throw new IllegalStateException("This Field cannot be converted to a DoubleValues due to the underlying data [" +
                    values.getClass().getSimpleName() + "]");
        }

        return new DoubleField(name, (FieldValues.Doubles) values);
    }

    @Override
    public double getDouble(double defaultValue) {
        return asDoubleField().getDouble(defaultValue);
    }

    @Override
    public BigIntegerField asBigIntegerField() {
        if (values instanceof FieldValues.BigIntegers == false) {
            throw new IllegalStateException("This Field cannot be converted to a BigIntegerField due to the underlying data [" +
                    values.getClass().getSimpleName() + "]");
        }

        return new BigIntegerField(name, (FieldValues.BigIntegers) values);
    }

    @Override
    public BigInteger getBigInteger(BigInteger defaultValue) {
        return asBigIntegerField().getBigInteger(defaultValue);
    }

    @Override
    public StringField asStringField() {
        if (values instanceof FieldValues.Strings == false) {
            throw new IllegalStateException("This Field cannot be converted to a StringField due to the underlying data [" +
                    values.getClass().getSimpleName() + "]");
        }

        return new StringField(name, (FieldValues.Strings) values);
    }

    @Override
    public String getString(String defaultValue) {
        return asStringField().getString(defaultValue);
    }

    @Override
    public DefField asDefField() {
        if (values instanceof FieldValues.Objects == false) {
            throw new IllegalStateException("This Field cannot be converted to a ObjectField due to the underlying data [" +
                    values.getClass().getSimpleName() + "]");
        }

        return new DefField(name, (FieldValues.Objects) values);
    }

    @Override
    public Object getDef(Object defaultValue) {
        return asDefField().getValue(defaultValue);
    }
}
