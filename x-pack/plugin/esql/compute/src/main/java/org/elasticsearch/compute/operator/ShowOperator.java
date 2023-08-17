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

public class ShowOperator extends LocalSourceOperator {

    public record ShowOperatorFactory(List<List<Object>> objects) implements SourceOperatorFactory {
        @Override
        public String describe() {
            return "ShowOperator[objects = " + objects.stream().map(Objects::toString).collect(joining(",")) + "]";
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new ShowOperator(() -> objects);
        }
    }

    public ShowOperator(ListSupplier listSupplier) {
        super(listSupplier);
    }
}
