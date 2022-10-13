/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.BinaryProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

public abstract class BinaryDateTimeProcessor extends BinaryProcessor {

    private final ZoneId zoneId;

    public BinaryDateTimeProcessor(Processor source1, Processor source2, ZoneId zoneId) {
        super(source1, source2);
        this.zoneId = zoneId;
    }

    public BinaryDateTimeProcessor(StreamInput in) throws IOException {
        super(in);
        zoneId = SqlStreamInput.asSqlStream(in).zoneId();
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {}

    ZoneId zoneId() {
        return zoneId;
    }

    @Override
    protected abstract Object doProcess(Object left, Object right);

    @Override
    public int hashCode() {
        return Objects.hash(left(), right(), zoneId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BinaryDateTimeProcessor other = (BinaryDateTimeProcessor) obj;
        return Objects.equals(left(), other.left()) && Objects.equals(right(), other.right()) && Objects.equals(zoneId(), other.zoneId());
    }
}
