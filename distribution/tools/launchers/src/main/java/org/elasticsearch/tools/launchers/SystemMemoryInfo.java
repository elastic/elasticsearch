/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

/**
 * Determines available system memory that could be allocated for Elasticsearch, to include JVM heap and other native processes.
 * The "available system memory" is defined as the total system memory which is visible to the Elasticsearch process. For instances
 * in which Elasticsearch is running in a containerized environment (i.e. Docker) this is expected to be the limits set for the container,
 * not the host system.
 */
public interface SystemMemoryInfo {

    /**
     *
     * @return total system memory available to heap or native process allocation in bytes
     * @throws SystemMemoryInfoException if unable to determine available system memory
     */
    long availableSystemMemory() throws SystemMemoryInfoException;

    class SystemMemoryInfoException extends Exception {
        public SystemMemoryInfoException(String message) {
            super(message);
        }
    }
}
