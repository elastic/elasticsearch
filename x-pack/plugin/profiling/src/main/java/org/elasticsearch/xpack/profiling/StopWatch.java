/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.apache.logging.log4j.Logger;

/**
 * Measures time and logs it in milliseconds.
 */
public class StopWatch {
    private final String name;
    private final long start;

    public StopWatch(String name) {
        this.name = name;
        start = System.nanoTime();
    }

    /**
     * Log name and the number of elapsed milliseconds since object creation.
     *
     * @param log Logger for logging the report.
     */
    public void Log(Logger log) {
        log.debug(this.Report());
    }

    /**
     * Return a textual report including the name and the number of elapsed milliseconds since object creation.
     */
    public String Report() {
        return name + " took [" + (System.nanoTime() - start) / 1_000_000.0d + " ms].";
    }

    /**
     * Return number of elapsed milliseconds since object creation.
     */
    public double Millis() {
        return (System.nanoTime() - start) / 1_000_000.0d;
    }

}
