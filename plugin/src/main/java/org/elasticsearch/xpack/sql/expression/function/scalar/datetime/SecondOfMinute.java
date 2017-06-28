/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.joda.time.ReadableDateTime;

public class SecondOfMinute extends DateTimeFunction {

    public SecondOfMinute(Location location, Expression argument) {
        super(location, argument);
    }

    @Override
    public String dateTimeFormat() {
        return "s";
    }

    @Override
    public String interval() {
        return "second";
    }

    @Override
    protected int extract(ReadableDateTime dt) {
        return dt.getSecondOfMinute();
    }
}
