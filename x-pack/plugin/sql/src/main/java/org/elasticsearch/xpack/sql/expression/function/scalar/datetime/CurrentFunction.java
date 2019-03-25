/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.function.scalar.ConfigurationFunction;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.time.ZonedDateTime;
import java.util.Objects;

abstract class CurrentFunction extends ConfigurationFunction {

    private final ZonedDateTime date;

    CurrentFunction(Source source, Configuration configuration, ZonedDateTime date, DataType dataType) {
        super(source, configuration, dataType);
        this.date = date;
    }

    @Override
    public Object fold() {
        return date;
    }

    @Override
    public int hashCode() {
        return Objects.hash(date);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CurrentFunction other = (CurrentFunction) obj;
        return Objects.equals(date, other.date);
    }
}
