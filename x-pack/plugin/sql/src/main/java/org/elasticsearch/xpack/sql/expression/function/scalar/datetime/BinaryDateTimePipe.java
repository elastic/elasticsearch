/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.Objects;

public abstract class BinaryDateTimePipe extends BinaryPipe {

    private final ZoneId zoneId;

    public BinaryDateTimePipe(Source source, Expression expression, Pipe left, Pipe right, ZoneId zoneId) {
        super(source, expression, left, right);
        this.zoneId = zoneId;
    }

    ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public Processor asProcessor() {
        return makeProcessor(left().asProcessor(), right().asProcessor(), zoneId);
    }

    protected abstract Processor makeProcessor(Processor left, Processor right, ZoneId zoneId);

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), zoneId);
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
        BinaryDateTimePipe that = (BinaryDateTimePipe) o;
        return Objects.equals(zoneId, that.zoneId);
    }
}
