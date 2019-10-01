/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.expression.gen.processor.BinaryProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.BinaryDateTimeProcessor.BinaryDateOperation.TRUNC;

public abstract class BinaryDateTimeProcessor extends BinaryProcessor {

    // TODO: Remove and in favour of inheritance (subclasses which implement abstract methods)
    public enum BinaryDateOperation {
        TRUNC,
        PART;
    }

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
    protected void doWrite(StreamOutput out) {
    }

    ZoneId zoneId() {
        return zoneId;
    }

    @Override
    protected abstract Object doProcess(Object left, Object right);

    public static BinaryDateTimeProcessor asProcessor(BinaryDateOperation operation, Processor left, Processor right, ZoneId zoneId) {
        if (operation == TRUNC) {
            return new DateTruncProcessor(left, right, zoneId);
        } else {
            return new DatePartProcessor(left, right, zoneId);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BinaryDateTimeProcessor that = (BinaryDateTimeProcessor) o;
        return zoneId.equals(that.zoneId);
    }
}
