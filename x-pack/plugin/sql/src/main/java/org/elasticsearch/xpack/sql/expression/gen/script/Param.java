/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.script;

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
}
