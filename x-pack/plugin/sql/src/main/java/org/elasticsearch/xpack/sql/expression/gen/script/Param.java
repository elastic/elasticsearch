/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.script;

import java.util.Locale;

abstract class Param<T> {
    private final T value;

    Param(T value) {
        this.value = value;
    }

    abstract String prefix();

    T value() {
        return value;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "{%s=%s}", prefix(), value);
    }
}
