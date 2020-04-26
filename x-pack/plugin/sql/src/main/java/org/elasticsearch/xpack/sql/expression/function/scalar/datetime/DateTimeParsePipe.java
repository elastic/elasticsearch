/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.DateTimeParseExtractor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

public class DateTimeParsePipe extends BinaryDateTimePipe {

    final private DateTimeParseExtractor dateTimeParseExtractor;

    public DateTimeParsePipe(Source source, Expression expression, Pipe left, Pipe right, DateTimeParseExtractor dateTimeParseExtractor) {
        super(source, expression, left, right, null);
        this.dateTimeParseExtractor = dateTimeParseExtractor;
    }

    @Override
    protected NodeInfo<DateTimeParsePipe> info() {
        return NodeInfo.create(this, DateTimeParsePipe::new, expression(), left(), right(), dateTimeParseExtractor);
    }

    @Override
    protected DateTimeParsePipe replaceChildren(Pipe left, Pipe right) {
        return new DateTimeParsePipe(source(), expression(), left, right, dateTimeParseExtractor);
    }

    @Override
    protected Processor makeProcessor(Processor left, Processor right, ZoneId zoneId) {
        return new DateTimeParseProcessor(left, right, dateTimeParseExtractor);
    }
}
