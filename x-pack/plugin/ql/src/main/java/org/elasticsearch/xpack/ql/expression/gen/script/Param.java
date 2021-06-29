/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.script;

import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

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
        return format(null, "{{}={}}", prefix(), value);
    }


    @Override
    public int hashCode() {
        if (this.value == null) {
            return Objects.hashCode(null);
        }
        return this.value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if ((obj instanceof  Param) == false) {
            return false;
        }
        if (this.value == null) {
            return ((Param)obj).value == null;
        }
        return this.value.equals(((Param)obj).value);
    }
}
