/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.FunctionContext;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;

import java.util.List;

/**
 * Extract the week of the year from a datetime.
 */
public class WeekOfYear extends NumberDateTimeFunction {

    public WeekOfYear(Location location, List<Expression> arguments, FunctionContext context) {
        super(location, arguments, context);
    }

    @Override
    NodeCtor2<List<Expression>, FunctionContext, DateTimeFunction> ctorForInfo() {
        return WeekOfYear::new;
    }

    @Override
    Expression replaceChildren(Location location, List<Expression> newChildren, FunctionContext context) {
        return new WeekOfYear(location, newChildren, context);
    }

    @Override
    DateTimeProcessor.DateTimeExtractor extractor() {
        return DateTimeProcessor.DateTimeExtractor.WEEK_OF_YEAR;
    }

    @Override
    public String dateTimeFormat() {
        return "w";
    }
}
