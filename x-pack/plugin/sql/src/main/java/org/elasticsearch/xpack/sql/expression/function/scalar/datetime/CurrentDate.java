/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.function.scalar.ConfigurationFunction;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.ZonedDateTime;
import java.util.Objects;

public class CurrentDate extends ConfigurationFunction {

    private final ZonedDateTime date;

    public CurrentDate(Source source, Configuration configuration) {
        super(source, configuration, DataType.DATE);
        ZonedDateTime zdt = configuration().now();
        if (zdt == null) {
            this.date = null;
        } else {
            this.date = DateUtils.asDateOnly(configuration().now());
        }
    }

    @Override
    public Object fold() {
        return date;
    }

    @Override
    protected NodeInfo<CurrentDate> info() {
        return NodeInfo.create(this, CurrentDate::new, configuration());
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

        CurrentDate other = (CurrentDate) obj;
        return Objects.equals(date, other.date);
    }
}
