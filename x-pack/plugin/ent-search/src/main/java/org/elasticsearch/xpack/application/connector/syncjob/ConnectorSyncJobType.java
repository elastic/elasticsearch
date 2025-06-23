/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import java.util.Locale;

public enum ConnectorSyncJobType {
    FULL,
    INCREMENTAL,
    ACCESS_CONTROL;

    public static ConnectorSyncJobType fromString(String syncJobTypeString) {
        for (ConnectorSyncJobType syncJobType : ConnectorSyncJobType.values()) {
            if (syncJobType.name().equalsIgnoreCase(syncJobTypeString)) {
                return syncJobType;
            }
        }

        throw new IllegalArgumentException("Unknown " + ConnectorSyncJobType.class.getSimpleName() + " [" + syncJobTypeString + "].");
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
