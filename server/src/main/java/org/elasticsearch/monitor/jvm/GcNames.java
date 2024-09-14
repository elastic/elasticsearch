/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.jvm;

public class GcNames {

    public static final String YOUNG = "young";
    public static final String OLD = "old";
    public static final String SURVIVOR = "survivor";

    /**
     * Resolves the GC type by its memory pool name ({@link java.lang.management.MemoryPoolMXBean#getName()}.
     */
    public static String getByMemoryPoolName(String poolName, String defaultName) {
        if ("Eden Space".equals(poolName)
            || "PS Eden Space".equals(poolName)
            || "Par Eden Space".equals(poolName)
            || "G1 Eden Space".equals(poolName)) {
            return YOUNG;
        }
        if ("Survivor Space".equals(poolName)
            || "PS Survivor Space".equals(poolName)
            || "Par Survivor Space".equals(poolName)
            || "G1 Survivor Space".equals(poolName)) {
            return SURVIVOR;
        }
        if ("Tenured Gen".equals(poolName)
            || "PS Old Gen".equals(poolName)
            || "CMS Old Gen".equals(poolName)
            || "G1 Old Gen".equals(poolName)) {
            return OLD;
        }
        return defaultName;
    }

    public static String getByGcName(String gcName, String defaultName) {
        if ("Copy".equals(gcName) || "PS Scavenge".equals(gcName) || "ParNew".equals(gcName) || "G1 Young Generation".equals(gcName)) {
            return YOUNG;
        }
        if ("MarkSweepCompact".equals(gcName)
            || "PS MarkSweep".equals(gcName)
            || "ConcurrentMarkSweep".equals(gcName)
            || "G1 Old Generation".equals(gcName)) {
            return OLD;
        }
        return defaultName;
    }
}
