/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Foldables;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.time.ZonedDateTime;

public class CurrentDateTime extends CurrentFunction {

    private final Expression precision;

    public CurrentDateTime(Source source, Expression precision, Configuration configuration) {
        super(source, configuration, nanoPrecision(configuration.now(), precision), DataType.DATETIME);
        this.precision = precision;
    }

    Expression precision() {
        return precision;
    }

    @Override
    protected NodeInfo<CurrentDateTime> info() {
        return NodeInfo.create(this, CurrentDateTime::new, precision, configuration());
    }

    static ZonedDateTime nanoPrecision(ZonedDateTime zdt, Expression precisionExpression) {
        int precision = precisionExpression != null ? Foldables.intValueOf(precisionExpression) : 3;
        int nano = zdt.getNano();
        if (precision >= 0 && precision < 10) {
            // remove the remainder
            nano = nano - nano % (int) Math.pow(10, (9 - precision));
            return zdt.withNano(nano);
        }
        return zdt;
    }
}
