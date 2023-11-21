/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import java.util.Locale;

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
