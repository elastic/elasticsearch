/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

public class InvalidConversion extends RuntimeException {
    protected final Class<?> from;
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
