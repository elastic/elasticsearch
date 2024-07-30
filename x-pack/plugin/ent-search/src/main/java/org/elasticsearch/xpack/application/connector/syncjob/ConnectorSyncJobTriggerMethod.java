/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import java.util.Locale;

public enum ConnectorSyncJobTriggerMethod {
    ON_DEMAND,
    SCHEDULED;

    public static ConnectorSyncJobTriggerMethod fromString(String triggerMethodString) {
        for (ConnectorSyncJobTriggerMethod triggerMethod : ConnectorSyncJobTriggerMethod.values()) {
            if (triggerMethod.name().equalsIgnoreCase(triggerMethodString)) {
                return triggerMethod;
            }
        }

        throw new IllegalArgumentException(
            "Unknown " + ConnectorSyncJobTriggerMethod.class.getSimpleName() + " [" + triggerMethodString + "]."
        );
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
