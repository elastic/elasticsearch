/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.ConfigurationFunction;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.time.ZonedDateTime;
import java.util.Objects;

public class CurrentDateTime extends ConfigurationFunction {
    private final Expression precision;
    private final ZonedDateTime dateTime;

    public CurrentDateTime(Source source, Expression precision, Configuration configuration) {
        super(source, configuration, DataType.DATE);
        this.precision = precision;
        int p = precision != null ? ((Number) precision.fold()).intValue() : 0;
        this.dateTime = nanoPrecision(configuration().now(), p);
    }

    @Override
    public Object fold() {
        return dateTime;
    }

    @Override
    protected NodeInfo<CurrentDateTime> info() {
        return NodeInfo.create(this, CurrentDateTime::new, precision, configuration());
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CurrentDateTime other = (CurrentDateTime) obj;
        return Objects.equals(dateTime, other.dateTime);
    }

    static ZonedDateTime nanoPrecision(ZonedDateTime zdt, int precision) {
        if (zdt != null) {
            int nano = zdt.getNano();
            if (precision >= 0 && precision < 10) {
                // remove the remainder
                nano = nano - nano % (int) Math.pow(10, (9 - precision));
                return zdt.withNano(nano);
            }
        }
        return zdt;
    }
}
