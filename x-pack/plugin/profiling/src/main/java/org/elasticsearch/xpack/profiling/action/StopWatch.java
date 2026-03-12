/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

/**
 * Measures time and logs it in milliseconds.
 */
public final class StopWatch {
    private final String name;
    private final long start;

    public StopWatch(String name) {
        this.name = name;
        start = System.nanoTime();
    }

    /**
     * Return a textual report including the name and the number of elapsed milliseconds since object creation.
     */
    public String report() {
        return name + " took [" + millis() + " ms].";
    }

    /**
     * Return number of elapsed milliseconds since object creation.
     */
    public double millis() {
        return (System.nanoTime() - start) / 1_000_000.0d;
    }
}
