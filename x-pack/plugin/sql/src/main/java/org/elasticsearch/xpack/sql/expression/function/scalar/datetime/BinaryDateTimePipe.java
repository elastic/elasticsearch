/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

import java.time.ZoneId;
import java.util.Objects;

public class BinaryDateTimePipe extends BinaryPipe {

    private final ZoneId zoneId;
    private final BinaryDateTimeProcessor.BinaryDateOperation operation;

    public BinaryDateTimePipe(Source source, Expression expression, Pipe left, Pipe right, ZoneId zoneId,
                              BinaryDateTimeProcessor.BinaryDateOperation operation) {
        super(source, expression, left, right);
        this.zoneId = zoneId;
        this.operation = operation;
    }

    ZoneId zoneId() {
        return zoneId;
    }

    BinaryDateTimeProcessor.BinaryDateOperation operation() {
        return operation;
    }

    @Override
    protected NodeInfo<BinaryDateTimePipe> info() {
        return NodeInfo.create(this, BinaryDateTimePipe::new, expression(), left(), right(), zoneId, operation);
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new BinaryDateTimePipe(source(), expression(), left, right, zoneId, operation);
    }

    @Override
    public Processor asProcessor() {
        return BinaryDateTimeProcessor.asProcessor(operation, left().asProcessor(), right().asProcessor(), zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), zoneId, operation);
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
        BinaryDateTimePipe that = (BinaryDateTimePipe) o;
        return Objects.equals(zoneId, that.zoneId) &&
            operation == that.operation;
    }
}
