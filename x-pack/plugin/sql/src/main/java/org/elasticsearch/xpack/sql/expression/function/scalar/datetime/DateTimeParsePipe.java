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
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.Parser;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.Objects;

public class DateTimeParsePipe extends BinaryDateTimePipe {

    private final Parser parser;

    public DateTimeParsePipe(Source source, Expression expression, Pipe left, Pipe right, ZoneId zoneId, Parser parser) {
        super(source, expression, left, right, zoneId);
        this.parser = parser;
    }

    @Override
    protected NodeInfo<DateTimeParsePipe> info() {
        return NodeInfo.create(this, DateTimeParsePipe::new, expression(), left(), right(), zoneId(), parser);
    }

    @Override
    protected DateTimeParsePipe replaceChildren(Pipe left, Pipe right) {
        return new DateTimeParsePipe(source(), expression(), left, right, zoneId(), parser);
    }

    @Override
    protected Processor makeProcessor(Processor left, Processor right, ZoneId zoneId) {
        return new DateTimeParseProcessor(left, right, zoneId, parser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.parser);
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
        DateTimeParsePipe that = (DateTimeParsePipe) o;
        return super.equals(o) && this.parser == that.parser;
    }

    public Parser parser() {
        return parser;
    }
}
