/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.common;

import org.elasticsearch.common.logging.LogConfigurator;

/**
 * An external bridge for the logging subsystem, exposing the minimum necessary
 * to wire up the log4j-based implementation that is present in Logstash.
 */
public class LoggingBridge {
    private LoggingBridge() {}

    public static void initialize() {
        // wires up the ES logging front-end to a Log4j backend
        LogConfigurator.configureESLogging();
    }
}
