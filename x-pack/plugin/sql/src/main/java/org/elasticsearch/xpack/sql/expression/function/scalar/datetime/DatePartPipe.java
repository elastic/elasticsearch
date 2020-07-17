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

public class DatePartPipe extends BinaryDateTimePipe {

    public DatePartPipe(Source source, Expression expression, Pipe left, Pipe right, ZoneId zoneId) {
        super(source, expression, left, right, zoneId);
    }

    @Override
    protected NodeInfo<DatePartPipe> info() {
        return NodeInfo.create(this, DatePartPipe::new, expression(), left(), right(), zoneId());
    }

    @Override
    protected DatePartPipe replaceChildren(Pipe left, Pipe right) {
        return new DatePartPipe(source(), expression(), left, right, zoneId());
    }

    @Override
    protected Processor makeProcessor(Processor left, Processor right, ZoneId zoneId) {
        return new DatePartProcessor(left, right, zoneId);
    }
}
