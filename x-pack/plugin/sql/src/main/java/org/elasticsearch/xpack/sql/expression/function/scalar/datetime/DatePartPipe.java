/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

import java.time.ZoneId;

public class DatePartPipe extends ThreeArgsDateTimePipe {

    public DatePartPipe(Source source, Expression expression, Pipe first, Pipe second, Pipe third, ZoneId zoneId) {
        super(source, expression, first, second, third, zoneId);
    }

    @Override
    protected NodeInfo<DatePartPipe> info() {
        return NodeInfo.create(this, DatePartPipe::new, expression(), first(), second(), third(), zoneId());
    }

    @Override
    public ThreeArgsDateTimePipe replaceChildren(Pipe newFirst, Pipe newSecond, Pipe newThird) {
        return new DatePartPipe(source(), expression(), newFirst, newSecond, newThird, zoneId());
    }

    @Override
    protected Processor makeProcessor(Processor first, Processor second, Processor third, ZoneId zoneId) {
        return new DatePartProcessor(first, second, third, zoneId);
    }
}
