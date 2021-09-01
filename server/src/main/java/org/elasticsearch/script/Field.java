/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */


package org.elasticsearch.script;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;

import java.math.BigInteger;
import java.util.List;

/**
 * A field in a document accessible via scripting.  In search contexts, the Field may be backed by doc values, source
 * or both.  In ingestion, the field may be in the source document or being added to the document.
 *
 * Field's methods must not throw exceptions nor return null.  A Field object representing a empty or unmapped field will have
 * * {@code isEmpty() == true}
 * * {@code getValues().equals(Collections.emptyList())}
 * * {@code getValue(defaultValue) == defaultValue}
 */
public abstract class Field<T> {
    public static final Converter<BigInteger, BigIntegerField> BigInteger = Converters.BIGINTEGER;
    public static final Converter<Long, LongField> Long = Converters.LONG;

    protected final String name;
    protected final FieldValues<T> values;

    public Field(String name, FieldValues<T> values) {
        this.name = name;
        this.values = values;
    }

    public String getName() {
        return name;
    }

    /**
     * Does the field have any values? An unmapped field may have values from source
     */
    public boolean isEmpty() {
        return values.isEmpty();
    }

    /**
     * Get all values of a multivalued field.  If {@code isEmpty()} this returns an empty list
     */
    public List<T> getValues() {
        return values.getValues();
    }

    /**
     * Convert this {@code Field} into another {@code Field} type using a {@link Converter} of the target type.
     *
     * As this is called from user scripts, {@code as} may be called to convert a field into its same type, if
     * so {@code this} is cast via converters {@link Converter#getFieldClass()}.
     *
     */
    public final <CT, CF extends Field<CT>> Field<CT> as(Converter<CT, CF> converter) {
        if (converter.getFieldClass().isInstance(this)) {
            return converter.getFieldClass().cast(this);
        }

        return convert(converter);
    }

    /**
     * Extensions outside the core package should override this method to implement their own conversions, calling
     * the superclass of this method as a fallback.
     */
    protected <CT, CF extends Field<CT>> Field<CT> convert(Converter<CT, CF> converter) {
        return converter.convert(this);
    }

    /**
     * Provide {@link Converter}s access to the underlying {@link FieldValues}, should not be exposed to scripts
     */
    public FieldValues<T> getFieldValues() {
        return values;
    }

    /** Get the first value of a field, if {@code isEmpty()} return defaultValue instead */
    public T getValue(T defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        try {
            return values.getNonPrimitiveValue();
        } catch (RuntimeException err) {
            return defaultValue;
        }
    }

    /**
     * Get the underlying value as a {@code double} unless {@link #isEmpty()}, in which case return {@code defaultValue}.
     * May throw {@link InvalidConversion} if the underlying value is not representable as a {@code double}.
     */
    public double getDouble(double defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        try {
            return values.getDoubleValue();
        } catch (RuntimeException err) {
            return defaultValue;
        }
    }


    /**
     * Get the underlying value as a {@code long} unless {@link #isEmpty()}, in which case return {@code defaultValue}.
     * May throw {@link InvalidConversion} if the underlying value is not representable as a {@code long}.
     */
    public long getLong(long defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        try {
            return values.getLongValue();
        } catch (RuntimeException err) {
            return defaultValue;
        }
    }

    public static class BooleanField extends Field<Boolean> {
        public BooleanField(String name, FieldValues<Boolean> values) {
            super(name, values);
        }
    }

    public static class DoubleField extends Field<Double> {
        public DoubleField(String name, FieldValues<Double> values) {
            super(name, values);
        }
    }

    public static class LongField extends Field<Long> {
        public LongField(String name, FieldValues<Long> values) {
            super(name, values);
        }
    }

    public static class DateNanosField extends Field<JodaCompatibleZonedDateTime> {
        public DateNanosField(String name, FieldValues<JodaCompatibleZonedDateTime> values) {
            super(name, values);
        }
    }

    public static class DateMillisField extends Field<JodaCompatibleZonedDateTime> {
        public DateMillisField(String name, FieldValues<JodaCompatibleZonedDateTime> values) {
            super(name, values);
        }
    }

    public static class GeoPointField extends Field<GeoPoint> {
        public GeoPointField(String name, FieldValues<GeoPoint> values) {
            super(name, values);
        }
    }

    public static class StringField extends Field<String> {
        public StringField(String name, FieldValues<String> values) {
            super(name, values);
        }
    }

    public static class BytesRefField extends Field<BytesRef> {
        public BytesRefField(String name, FieldValues<BytesRef> values) {
            super(name, values);
        }
    }

    public static class BigIntegerField extends Field<BigInteger> {
        public BigIntegerField(String name, FieldValues<BigInteger> values) {
            super(name, values);
        }
    }

    public static class VersionField extends Field<String> {
        public VersionField(String name, FieldValues<String> values) {
            super(name, values);
        }
    }

    public static class IpField extends Field<String> {
        public IpField(String name, FieldValues<String> values) {
            super(name, values);
        }
    }
}
