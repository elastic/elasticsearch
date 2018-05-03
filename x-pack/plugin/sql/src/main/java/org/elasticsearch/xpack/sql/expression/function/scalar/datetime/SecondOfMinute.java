/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.FunctionContext;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;

import java.util.List;

/**
 * Extract the second of the minute from a datetime.
 */
public class SecondOfMinute extends NumberDateTimeFunction {
    public SecondOfMinute(Location location, List<Expression> arguments, FunctionContext context) {
        super(location, arguments, context);
    }

    @Override
    NodeCtor2<List<Expression>, FunctionContext, DateTimeFunction> ctorForInfo() {
        return SecondOfMinute::new;
    }

    @Override
    public Expression replaceChildren(Location location, List<Expression> newChildren, FunctionContext context) {
        return new SecondOfMinute(location, newChildren, context);
    }

    @Override
    public String dateTimeFormat() {
        return "s";
    }

    @Override
    DateTimeExtractor extractor() {
        return DateTimeExtractor.SECOND_OF_MINUTE;
    }
}
