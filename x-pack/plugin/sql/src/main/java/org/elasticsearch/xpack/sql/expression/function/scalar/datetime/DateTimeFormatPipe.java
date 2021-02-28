/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFormatProcessor.Formatter;

public class DateTimeFormatPipe extends BinaryDateTimePipe {

    private final Formatter formatter;

    public DateTimeFormatPipe(Source source, Expression expression, Pipe left, Pipe right, ZoneId zoneId, Formatter formatter) {
        super(source, expression, left, right, zoneId);
        this.formatter = formatter;
    }

    @Override
    protected NodeInfo<DateTimeFormatPipe> info() {
        return NodeInfo.create(this, DateTimeFormatPipe::new, expression(), left(), right(), zoneId(), formatter);
    }

    @Override
    protected DateTimeFormatPipe replaceChildren(Pipe left, Pipe right) {
        return new DateTimeFormatPipe(source(), expression(), left, right, zoneId(), formatter);
    }

    @Override
    protected Processor makeProcessor(Processor left, Processor right, ZoneId zoneId) {
        return new DateTimeFormatProcessor(left, right, zoneId, formatter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.formatter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        DateTimeFormatPipe other = (DateTimeFormatPipe) o;
        return super.equals(o) && this.formatter == other.formatter;
    }

    public Formatter formatter() {
        return formatter;
    }
}
