/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Measures time and logs it in milliseconds.
 */
public class StopWatch {
    private static final Logger log = LogManager.getLogger(TransportGetStackTracesAction.class);
    private final long start;

    public StopWatch() {
        start = System.nanoTime();
    }

    /**
     * Print name and the number of elapsed milliseconds since object creation.
     *
     * @param name String to be printed with the number of milliseconds taken.
     */
    public void Log(String name) {
        log.debug(name + " took [" + (System.nanoTime() - start) / 1_000_000.0d + " ms].");
    }

    /**
     * Return number of elapsed milliseconds since object creation.
     */
    public double Millis() {
        return (System.nanoTime() - start) / 1_000_000.0d;
    }

}
