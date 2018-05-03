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
 * Extract the year from a datetime.
 */
public class Year extends DateTimeHistogramFunction {
    public Year(Location location, List<Expression> arguments, FunctionContext context) {
        super(location, arguments, context);
    }

    @Override
    NodeCtor2<List<Expression>, FunctionContext, DateTimeFunction> ctorForInfo() {
        return Year::new;
    }

    @Override
    Expression replaceChildren(Location location, List<Expression> newChildren, FunctionContext context) {
        return new Year(location, newChildren, context);
    }

    @Override
    public String dateTimeFormat() {
        return "year";
    }

    @Override
    public Expression orderBy() {
        return arguments().get(0);
    }

    @Override
    protected DateTimeExtractor extractor() {
        return DateTimeExtractor.YEAR;
    }

    @Override
    public String interval() {
        return "year";
    }
}
