/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.Describable;

public record DriverFactory(String sessionId, DriverSupplier driverSupplier, DriverParallelism driverParallelism) implements Describable {

    @Override
    public String describe() {
        return "DriverFactory(instances = "
            + driverParallelism.instanceCount()
            + ", type = "
            + driverParallelism.type()
            + ")\n"
            + driverSupplier.describe();
    }

    public Driver createDriver() {
        return driverSupplier.create(sessionId);
    }

    public interface DriverSupplier extends Describable {
        Driver create(String sessionId);
    }
}
