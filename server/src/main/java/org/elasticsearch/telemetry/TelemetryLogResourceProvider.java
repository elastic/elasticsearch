/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry;

/**
 * Identifies the source of every OTel log record this node exports.
 */
public interface TelemetryLogResourceProvider {

    String DEFAULT_SERVICE_NAME = "elasticsearch";

    /** Must be non-null and non-empty. */
    String serviceName();

    /** Identity used when no plugin overrides it. */
    class Default implements TelemetryLogResourceProvider {
        @Override
        public String serviceName() {
            return DEFAULT_SERVICE_NAME;
        }
    }
}
