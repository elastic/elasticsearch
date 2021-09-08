/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.expression.function.scalar.SqlConfigurationFunction;

import java.time.temporal.Temporal;
import java.util.Objects;

abstract class CurrentFunction<T extends Temporal> extends SqlConfigurationFunction {

    private final T current;

    CurrentFunction(Source source, Configuration configuration, T current, DataType dataType) {
        super(source, configuration, dataType);
        this.current = current;
    }

    @Override
    public Object fold() {
        return current;
    }

    @Override
    public int hashCode() {
        return Objects.hash(current);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CurrentFunction other = (CurrentFunction) obj;
        return Objects.equals(current, other.current);
    }
}
