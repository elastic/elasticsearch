/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

/**
 * A failed conversion of a script {@link Field}.  Thrown by {@link Converter}s and
 * {@link FieldValues#getDoubleValue()}/{@link FieldValues#getLongValue()}.
 */
public class InvalidConversion extends RuntimeException {
    /** Source {@link Field} or underlying value */
    protected final Class<?> from;

    /** Destination class or {@link Converter} */
    protected final Class<?> converter;

    public InvalidConversion(Class<?> from, Class<?> converter) {
        this.from = from;
        this.converter = converter;
    }

    @Override
    public String getMessage() {
        return "Cannot convert from [" + from.getSimpleName() + "] using converter [" + converter.getSimpleName() + "]";
    }
}
