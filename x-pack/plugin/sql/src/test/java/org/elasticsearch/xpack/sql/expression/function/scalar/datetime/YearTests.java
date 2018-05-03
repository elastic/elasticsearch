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

public class YearTests extends DateTimeFunctionTestcase<Year> {

    public void test1970() {
        processAndCheck(dateTime(0), UTC, 1970);
    }

    public void test2017() {
        processAndCheck(dateTime(2017, 1, 1), UTC, 2017);
    }

    public void test2000() {
        processAndCheck(dateTime(2000, 1, 1), UTC, 2000);
    }

    @Override
    Year build(Object value, FunctionContext context) {
        return new Year(null,
            Collections.singletonList(new Literal(null, value, DataType.DATE)),
            context);
    }

    Locale defaultLocale() {
        return Locale.ENGLISH;
    }
}
