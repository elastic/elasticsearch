/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

public class DateTimeFormatPipe extends BinaryDateTimePipe {

    public DateTimeFormatPipe(Source source, Expression expression, Pipe left, Pipe right, ZoneId zoneId) {
        super(source, expression, left, right, zoneId);
    }

    @Override
    protected NodeInfo<DateTimeFormatPipe> info() {
        return NodeInfo.create(this, DateTimeFormatPipe::new, expression(), left(), right(), zoneId());
    }

    @Override
    protected DateTimeFormatPipe replaceChildren(Pipe left, Pipe right) {
        return new DateTimeFormatPipe(source(), expression(), left, right, zoneId());
    }

    @Override
    protected Processor makeProcessor(Processor left, Processor right, ZoneId zoneId) {
        return new DateTimeFormatProcessor(left, right, zoneId);
    }
}
