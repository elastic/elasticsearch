/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.data.BlockBuilder.newConstantBytesRefBlockWith;
import static org.elasticsearch.compute.data.BlockBuilder.newConstantDoubleBlockWith;
import static org.elasticsearch.compute.data.BlockBuilder.newConstantIntBlockWith;
import static org.elasticsearch.compute.data.BlockBuilder.newConstantLongBlockWith;
import static org.elasticsearch.compute.data.BlockBuilder.newConstantNullBlockWith;

public class RowOperator extends SourceOperator {

    private final List<Object> objects;

    boolean finished;

    public record RowOperatorFactory(List<Object> objects) implements SourceOperatorFactory {

        @Override
        public SourceOperator get() {
            return new RowOperator(objects);
        }

        @Override
        public String describe() {
            return "RowOperator(objects = " + objects.stream().map(Objects::toString).collect(joining(",")) + ")";
        }
    }

    public RowOperator(List<Object> objects) {
        this.objects = objects;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public Page getOutput() {
        Block[] blocks = new Block[objects.size()];
        for (int i = 0; i < objects.size(); i++) {
            Object object = objects.get(i);
            if (object instanceof Integer intVal) {
                blocks[i] = newConstantIntBlockWith(intVal, 1);
            } else if (object instanceof Long longVal) {
                blocks[i] = newConstantLongBlockWith(longVal, 1);
            } else if (object instanceof Double doubleVal) {
                blocks[i] = newConstantDoubleBlockWith(doubleVal, 1);
            } else if (object instanceof String stringVal) {
                blocks[i] = newConstantBytesRefBlockWith(new BytesRef(stringVal), 1);
            } else if (object == null) {
                blocks[i] = newConstantNullBlockWith(1);
            } else {
                throw new UnsupportedOperationException();
            }
        }
        finished = true;
        return new Page(blocks);
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("objects=").append(objects);
        sb.append("]");
        return sb.toString();
    }
}
