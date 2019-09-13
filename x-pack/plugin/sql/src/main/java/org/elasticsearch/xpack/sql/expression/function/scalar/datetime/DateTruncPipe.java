/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

import java.time.ZoneId;
import java.util.Objects;

public class DateTruncPipe extends BinaryPipe {

    private final ZoneId zoneId;

    public DateTruncPipe(Source source, Expression expression, Pipe left, Pipe right, ZoneId zoneId) {
        super(source, expression, left, right);
        this.zoneId = zoneId;
    }

    ZoneId zoneId() {
        return zoneId;
    }

    @Override
    protected NodeInfo<DateTruncPipe> info() {
        return NodeInfo.create(this, DateTruncPipe::new, expression(), left(), right(), zoneId);
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new DateTruncPipe(source(), expression(), left, right, zoneId);
    }

    @Override
    public DateTruncProcessor asProcessor() {
        return new DateTruncProcessor(left().asProcessor(), right().asProcessor(), zoneId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DateTruncPipe that = (DateTruncPipe) o;
        return zoneId.equals(that.zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), zoneId);
    }
}
