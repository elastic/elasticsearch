/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

/**
 * The count and type of driver parallelism.
 */
public record DriverParallelism(Type type, int instanceCount) {

    public DriverParallelism {
        if (instanceCount <= 0) {
            throw new IllegalArgumentException("instance count must be greater than zero; got: " + instanceCount);
        }
    }

    public static final DriverParallelism SINGLE = new DriverParallelism(Type.SINGLETON, 1);

    public enum Type {
        SINGLETON,
        DATA_PARALLELISM,
        TASK_LEVEL_PARALLELISM
    }
}
