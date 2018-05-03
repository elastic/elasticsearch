/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.FunctionContext;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Collections;
import java.util.Locale;

public class DayOfMonthTests extends DateTimeFunctionTestcase<DayOfMonth> {

    public void test19700101() {
        processAndCheck(dateTime(0), UTC, 1);
    }

    public void test20170102() {
        processAndCheck(dateTime(2017, 1, 2), UTC, 2);
    }

    public void test20170131() {
        processAndCheck(dateTime(2017,1,31), UTC, 31);
    }

    @Override
    DayOfMonth build(Object value, FunctionContext context) {
        return new DayOfMonth(null,
            Collections.singletonList(new Literal(null, value, DataType.DATE)),
            context);
    }

    Locale defaultLocale() {
        return Locale.ENGLISH;
    }
}
