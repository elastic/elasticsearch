/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */


package org.elasticsearch.script;

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
 * @param <T>
 */
public interface Field<T> {
    /** The field name */
    String getName();

    /** Does the field have any values? An unmapped field may have values from source */
    boolean isEmpty();

    /** Get all values of a multivalued field.  If {@code isEmpty()} this returns an empty list. */
    List<T> getValues();

    /** Get first value, if it exists, or return {@code defaultValue}.  Returns def type, use the {@code get<Type>}
     * to avoid boxing
     */
    Object getValue(Object defaultValue);

    /** Treat the current {@code Field} as if it held primitive {@code long}s, throws {@code IllegalStateException} if impossible */
    LongField asLongField();
    long getLong(long defaultValue);

    /** Treat the current {@code Field} as if it held primitive {@code double}s, {@code throws IllegalStateException} if impossible */
    DoubleField asDoubleField();
    double getDouble(double defaultValue);

    /**
     * Treat the current {@code Field} as if it held {@code BigInteger}, throws {@code IllegalStateException} if underlying type does not
     * naturally contain {@code BigInteger}s.  If underlying values fit in a signed {@code long}, {@code asLongField} should be used.
     **/
    BigIntegerField asBigIntegerField();
    BigInteger getBigInteger(BigInteger defaultValue);

    /** Treat the current {@code Field} as if it held {@code Strings}s, throws {@code IllegalStateException} if impossible */
    StringField asStringField();
    String getString(String defaultValue);

    /**
     * Treat the current Field as if it held {@code Object}.  This is a way to break out of the Fields API and
     * allow the caller to do their own casting if necessary.
     */
    DefField asDefField();
    Object getDef(Object defaultValue);
}
