/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import java.util.Locale;

/**
 * Enum representing the synchronization status of a Connector:
 * <ul>
 *     <li><b>CANCELING</b>: The synchronization process is in the process of being canceled.</li>
 *     <li><b>CANCELED</b>: The synchronization process has been canceled before completion.</li>
 *     <li><b>COMPLETED</b>: The synchronization process has completed successfully.</li>
 *     <li><b>ERROR</b>: The synchronization process encountered an error and may not have completed successfully.</li>
 *     <li><b>IN_PROGRESS</b>: The synchronization process is currently in progress.</li>
 *     <li><b>PENDING</b>: The synchronization process is scheduled and waiting to start.</li>
 *     <li><b>SUSPENDED</b>: The synchronization process has been suspended.</li>
 * </ul>
 */
public enum ConnectorSyncStatus {
    CANCELING,
    CANCELED,
    COMPLETED,
    ERROR,
    IN_PROGRESS,
    PENDING,
    SUSPENDED;

    public static ConnectorSyncStatus fromString(String syncStatusString) {
        for (ConnectorSyncStatus syncStatus : ConnectorSyncStatus.values()) {
            if (syncStatus.toString().equalsIgnoreCase(syncStatusString)) {
                return syncStatus;
            }
        }

        throw new IllegalArgumentException("Unknown " + ConnectorSyncStatus.class.getSimpleName() + " [" + syncStatusString + "].");
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static ConnectorSyncStatus connectorSyncStatus(String status) {
        for (ConnectorSyncStatus connectorSyncStatus : ConnectorSyncStatus.values()) {
            if (connectorSyncStatus.name().equalsIgnoreCase(status)) {
                return connectorSyncStatus;
            }
        }
        throw new IllegalArgumentException("Unknown " + ConnectorSyncStatus.class.getSimpleName() + " [" + status + "].");
    }
}
