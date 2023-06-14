/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.joining;

public class RowOperator extends LocalSourceOperator {

    private final List<Object> objects;

    public record RowOperatorFactory(List<Object> objects) implements SourceOperatorFactory {

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new RowOperator(objects);
        }

        @Override
        public String describe() {
            return "RowOperator[objects = " + objects.stream().map(Objects::toString).collect(joining(",")) + "]";
        }
    }

    public RowOperator(List<Object> objects) {
        super(() -> objects);
        this.objects = objects;
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
