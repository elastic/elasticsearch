/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

/**
 * Utility class for PromQL-related functionality.
 */
public final class PromqlFeatures {

    private PromqlFeatures() {
        // Utility class - no instances
    }

    /**
     * Returns whether PromQL functionality is enabled.
     * Exists to provide a single point of change and minimize noise when upgrading capability versions.
     */
    public static boolean isEnabled() {
        return EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled() && EsqlCapabilities.Cap.PROMQL_V0.isEnabled();
    }
}
