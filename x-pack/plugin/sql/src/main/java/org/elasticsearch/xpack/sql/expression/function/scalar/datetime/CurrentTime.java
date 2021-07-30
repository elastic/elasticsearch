/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.time.OffsetTime;

import static org.elasticsearch.xpack.sql.util.DateUtils.getNanoPrecision;

public class CurrentTime extends CurrentFunction<OffsetTime> {

    private final Expression precision;

    public CurrentTime(Source source, Expression precision, Configuration configuration) {
        super(source, configuration, nanoPrecision(configuration.now().toOffsetDateTime().toOffsetTime(), precision),
            SqlDataTypes.TIME);
        this.precision = precision;
    }

    Expression precision() {
        return precision;
    }

    @Override
    protected NodeInfo<CurrentTime> info() {
        return NodeInfo.create(this, CurrentTime::new, precision, configuration());
    }

    static OffsetTime nanoPrecision(OffsetTime ot, Expression precisionExpression) {
        return ot.withNano(getNanoPrecision(precisionExpression, ot.getNano()));
    }

}
