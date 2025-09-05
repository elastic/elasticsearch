/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import java.util.Locale;

/**
 * Interface for plan nodes that need to be accounted in the statistics
 */
public interface TelemetryAware {

    /**
     * @return the label reported in the telemetry data. Only needs to be overwritten if the label doesn't match the class name.
     */
    default String telemetryLabel() {
        return getClass().getSimpleName().toUpperCase(Locale.ROOT);
    }
}
