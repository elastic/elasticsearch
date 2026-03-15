/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.service.windows;

/**
 * Represents the status of a Windows service as returned by the SCM.
 *
 * @param state the current state (one of the {@code SERVICE_*} constants, e.g. {@link #STOPPED})
 * @param win32ExitCode the Win32 exit code reported by the service
 * @param checkPoint a progress value during lengthy operations
 * @param waitHint estimated time in milliseconds for a pending operation
 */
public record ServiceStatus(int state, int win32ExitCode, int checkPoint, int waitHint) {

    public static final int STOPPED = Advapi32.SERVICE_STOPPED;
    public static final int START_PENDING = Advapi32.SERVICE_START_PENDING;
    public static final int STOP_PENDING = Advapi32.SERVICE_STOP_PENDING;
    public static final int RUNNING = Advapi32.SERVICE_RUNNING;

    /**
     * Synthetic state indicating that the service status could not be queried.
     * This value is never returned by the Windows SCM API.
     */
    public static final int UNKNOWN = -1;

    public String stateName() {
        return switch (state) {
            case STOPPED -> "STOPPED";
            case START_PENDING -> "START_PENDING";
            case STOP_PENDING -> "STOP_PENDING";
            case RUNNING -> "RUNNING";
            case UNKNOWN -> "UNKNOWN";
            default -> "UNKNOWN(" + state + ")";
        };
    }

    /**
     * Creates a ServiceStatus representing a failed query.
     */
    public static ServiceStatus unknown() {
        return new ServiceStatus(UNKNOWN, 0, 0, 0);
    }
}
