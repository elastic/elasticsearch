/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

public abstract class ThreeArgsDateTimeProcessor implements Processor {

    private final Processor first, second, third;
    private final ZoneId zoneId;

    public ThreeArgsDateTimeProcessor(Processor first, Processor second, Processor third, ZoneId zoneId) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.zoneId = zoneId;
    }

    protected ThreeArgsDateTimeProcessor(StreamInput in) throws IOException {
        this.first = in.readNamedWriteable(Processor.class);
        this.second = in.readNamedWriteable(Processor.class);
        this.third = in.readNamedWriteable(Processor.class);
        zoneId = SqlStreamInput.asSqlStream(in).zoneId();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(first);
        out.writeNamedWriteable(second);
        out.writeNamedWriteable(third);
    }

    public Processor first() {
        return first;
    }

    public Processor second() {
        return second;
    }

    public Processor third() {
        return third;
    }

    ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public Object process(Object input) {
        Object o1 = first().process(input);
        if (o1 == null) {
            return null;
        }
        Object o2 = second().process(input);
        if (o2 == null) {
            return null;
        }
        Object o3 = third().process(input);
        if (o3 == null) {
            return null;
        }

        return doProcess(o1, o2, o3, zoneId());
    }

    public abstract Object doProcess(Object o1, Object o2, Object o3, ZoneId zoneId);

    @Override
    public int hashCode() {
        return Objects.hash(first, second, third, zoneId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ThreeArgsDateTimeProcessor that = (ThreeArgsDateTimeProcessor) o;
        return Objects.equals(first, that.first) &&
            Objects.equals(second, that.second) &&
            Objects.equals(third, that.third) &&
            Objects.equals(zoneId, that.zoneId);
    }
}
