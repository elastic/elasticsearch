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
 * Interface for controlling Windows services via the Service Control Manager.
 *
 * <p>The default implementation ({@link NativeWindowsServiceControl}) uses Panama FFI to call
 * the Windows SCM APIs directly. Tests can provide mock implementations.
 */
public interface WindowsServiceControl {

    /**
     * Starts the specified service.
     * @param serviceId the service name
     * @throws WindowsServiceException if the service could not be started
     */
    void startService(String serviceId) throws WindowsServiceException;

    /**
     * Sends a stop control code to the specified service.
     * This returns after the stop has been initiated; the service may still be in STOP_PENDING state.
     * @param serviceId the service name
     * @throws WindowsServiceException if the stop command could not be sent
     */
    void stopService(String serviceId) throws WindowsServiceException;

    /**
     * Marks the specified service for deletion from the SCM database.
     * @param serviceId the service name
     * @throws WindowsServiceException if the service could not be deleted
     */
    void deleteService(String serviceId) throws WindowsServiceException;

    /**
     * Queries the current status of the specified service.
     * @param serviceId the service name
     * @return the current service status
     * @throws WindowsServiceException if the status could not be queried
     */
    ServiceStatus queryStatus(String serviceId) throws WindowsServiceException;

    /**
     * Creates the default native implementation backed by Panama FFI calls to advapi32.dll.
     */
    static WindowsServiceControl create() {
        return new NativeWindowsServiceControl();
    }
}
