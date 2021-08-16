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
 * The underlying contents of a scripting Field.  Implementations include
 * {@link org.elasticsearch.index.fielddata.ScriptDocValues}, {@code _source}, runtime fields, or
 * the converted form of the these values from a {@link Converter}.
 */
public interface FieldValues<T> {
    /** Are there any values? */
    boolean isEmpty();

    /** How many values? */
    int size();

    /** All underlying values.  Note this boxes primitives */
    List<T> getValues();

    /** The first value as a subclass of {@link java.lang.Object}.  Boxes primitives */
    T getNonPrimitiveValue();

    /** The first value as a primitive long.  For performance reasons, implementations should avoid intermediate boxings if possible */
    long getLongValue();

    /** The first value as a primitive double.  For performance reasons, implementations should avoid intermediate boxings if possible */
    double getDoubleValue();
}
