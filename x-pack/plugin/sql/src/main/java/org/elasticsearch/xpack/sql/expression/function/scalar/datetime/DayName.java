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
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;

/**
 * Extract the day name from a datetime.
 * <pre>
 * DATENAME("2017-12-05T00:00:00")
 * returns "Tuesday"
 * </pre>
 *
 */
public class DayName extends DateTimeFunction {

    // DateTimeFormatter.ofPattern
    static final String FORMAT = "EEEE";

    public DayName(Location location, List<Expression> arguments, FunctionContext context) {
        super(location, arguments, context);
    }

    @Override
    NodeCtor2<List<Expression>, FunctionContext, DateTimeFunction> ctorForInfo() {
        return DayName::new;
    }

    @Override
    Expression replaceChildren(Location location, List<Expression> newChildren, FunctionContext context) {
        return new DayName(location, newChildren, context);
    }

    @Override
    DateTimeExtractor extractor() {
        return DateTimeExtractor.DAYNAME;
    }

    @Override
    public String dateTimeFormat() {
        return FORMAT;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }
}
