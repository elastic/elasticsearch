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
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class ThreeArgsDateTimePipe extends Pipe {

    private final Pipe first, second, third;
    private final ZoneId zoneId;

    public ThreeArgsDateTimePipe(Source source, Expression expression, Pipe first, Pipe second, Pipe third, ZoneId zoneId) {
        super(source, expression, Arrays.asList(first, second, third));
        this.first = first;
        this.second = second;
        this.third = third;
        this.zoneId = zoneId;
    }

    public Pipe first() {
        return first;
    }

    public Pipe second() {
        return second;
    }

    public Pipe third() {
        return third;
    }

    ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    public abstract Pipe replaceChildren(Pipe newFirst, Pipe newSecond, Pipe newThird);

    @Override
    public Processor asProcessor() {
        return makeProcessor(first.asProcessor(), second.asProcessor(), third.asProcessor(), zoneId);
    }

    protected abstract Processor makeProcessor(Processor first, Processor second, Processor third, ZoneId zoneId);

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), first, second, third, zoneId);
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
        ThreeArgsDateTimePipe that = (ThreeArgsDateTimePipe) o;
        return first.equals(that.first) &&
            second.equals(that.second) &&
            third.equals(that.third) &&
            zoneId.equals(that.zoneId);
    }
}
