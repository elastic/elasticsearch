/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.service.windows;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_INT;

/**
 * Native implementation of {@link WindowsServiceControl} using Panama FFI calls to {@code advapi32.dll}.
 *
 * <p>Each operation opens handles to the SCM and service, performs the operation, and closes
 * the handles in a try-finally block to prevent leaks.
 */
class NativeWindowsServiceControl implements WindowsServiceControl {

    private final Advapi32 advapi32;

    NativeWindowsServiceControl() {
        this.advapi32 = new Advapi32();
    }

    @Override
    public void startService(String serviceId) throws WindowsServiceException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment scManager = openScManager();
            try {
                MemorySegment service = openService(scManager, serviceId, arena);
                try {
                    if (advapi32.startService(service) == false) {
                        throw new WindowsServiceException("Failed to start service '" + serviceId + "'", advapi32.getLastError());
                    }
                } finally {
                    advapi32.closeServiceHandle(service);
                }
            } finally {
                advapi32.closeServiceHandle(scManager);
            }
        }
    }

    @Override
    public void stopService(String serviceId) throws WindowsServiceException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment scManager = openScManager();
            try {
                MemorySegment service = openService(scManager, serviceId, arena);
                try {
                    MemorySegment serviceStatus = arena.allocate(Advapi32.SERVICE_STATUS_LAYOUT);
                    if (advapi32.controlService(service, Advapi32.SERVICE_CONTROL_STOP, serviceStatus) == false) {
                        throw new WindowsServiceException("Failed to stop service '" + serviceId + "'", advapi32.getLastError());
                    }
                } finally {
                    advapi32.closeServiceHandle(service);
                }
            } finally {
                advapi32.closeServiceHandle(scManager);
            }
        }
    }

    @Override
    public void deleteService(String serviceId) throws WindowsServiceException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment scManager = openScManager();
            try {
                MemorySegment service = openService(scManager, serviceId, arena);
                try {
                    if (advapi32.deleteService(service) == false) {
                        throw new WindowsServiceException("Failed to delete service '" + serviceId + "'", advapi32.getLastError());
                    }
                } finally {
                    advapi32.closeServiceHandle(service);
                }
            } finally {
                advapi32.closeServiceHandle(scManager);
            }
        }
    }

    @Override
    public ServiceStatus queryStatus(String serviceId) throws WindowsServiceException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment scManager = openScManager();
            try {
                MemorySegment service = openService(scManager, serviceId, arena);
                try {
                    MemorySegment bytesNeeded = arena.allocate(JAVA_INT);
                    // Call QueryServiceStatusEx with a NULL buffer to get the required buffer size
                    advapi32.queryServiceStatusEx(service, MemorySegment.NULL, 0, bytesNeeded);
                    var lastError = advapi32.getLastError();
                    if (lastError != Advapi32.ERROR_INSUFFICIENT_BUFFER) {
                        throw new WindowsServiceException("Failed to query status of service '" + serviceId + "'", lastError);
                    }

                    // Then call it again with a correctly sized buffer
                    var bufferSize = bytesNeeded.get(JAVA_INT, 0);
                    MemorySegment buffer = arena.allocate(bufferSize);
                    if (advapi32.queryServiceStatusEx(service, buffer, bufferSize, bytesNeeded) == false) {
                        throw new WindowsServiceException("Failed to query status of service '" + serviceId + "'", lastError);
                    }
                    return new ServiceStatus(
                        (int) Advapi32.dwCurrentState$vh.get(buffer),
                        (int) Advapi32.dwWin32ExitCode$vh.get(buffer),
                        (int) Advapi32.dwCheckPoint$vh.get(buffer),
                        (int) Advapi32.dwWaitHint$vh.get(buffer)
                    );
                } finally {
                    advapi32.closeServiceHandle(service);
                }
            } finally {
                advapi32.closeServiceHandle(scManager);
            }
        }
    }

    private MemorySegment openScManager() throws WindowsServiceException {
        MemorySegment handle = advapi32.openSCManager(Advapi32.SC_MANAGER_ALL_ACCESS);
        if (handle.equals(MemorySegment.NULL) || handle.address() == 0) {
            throw new WindowsServiceException("Failed to open Service Control Manager", advapi32.getLastError());
        }
        return handle;
    }

    private MemorySegment openService(MemorySegment scManager, String serviceId, Arena arena) throws WindowsServiceException {
        MemorySegment serviceName = PanamaUtil.allocateWideString(arena, serviceId);
        MemorySegment handle = advapi32.openService(scManager, serviceName, Advapi32.SERVICE_ALL_ACCESS);
        if (handle.equals(MemorySegment.NULL) || handle.address() == 0) {
            throw new WindowsServiceException("Failed to open service '" + serviceId + "'", advapi32.getLastError());
        }
        return handle;
    }
}
