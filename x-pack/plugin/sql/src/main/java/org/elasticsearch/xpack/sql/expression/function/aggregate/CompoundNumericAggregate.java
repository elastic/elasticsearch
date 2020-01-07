/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.CompoundAggregate;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

// marker type for compound aggregates, that is aggregate that provide multiple values (like Stats or Matrix)
// and thus cannot be used directly in SQL and are mainly for internal use
public abstract class CompoundNumericAggregate extends NumericAggregate implements CompoundAggregate {

    CompoundNumericAggregate(Source source, Expression field, List<Expression> arguments) {
        super(source, field, arguments);
    }

    CompoundNumericAggregate(Source source, Expression field) {
        super(source, field);
    }
}
