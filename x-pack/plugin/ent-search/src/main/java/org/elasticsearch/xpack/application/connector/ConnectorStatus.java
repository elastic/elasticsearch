/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import java.util.Locale;

/**
 * Enum representing the various states of a Connector:
 * <ul>
 *     <li><b>CREATED</b>: The connector has been created but is not yet configured.</li>
 *     <li><b>NEEDS_CONFIGURATION</b>: The connector requires further configuration to become operational.</li>
 *     <li><b>CONFIGURED</b>: The connector has been configured but has not yet established a connection.</li>
 *     <li><b>CONNECTED</b>: The connector is successfully connected and operational.</li>
 *     <li><b>ERROR</b>: The connector encountered an error and may not be operational.</li>
 * </ul>
 */
public enum ConnectorStatus {
    CREATED,
    NEEDS_CONFIGURATION,
    CONFIGURED,
    CONNECTED,
    ERROR;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
