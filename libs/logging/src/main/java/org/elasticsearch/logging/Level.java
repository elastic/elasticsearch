/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

/**
 * Levels used for identifying the severity of an event and filtering the logging output.
 */
public enum Level {

    /**
     * No events will be logged.
     */
    OFF,
    /**
     * A fatal error that will prevent the application from continuing.
     */
    FATAL,
    /**
     * An error in the application, possibly recoverable.
     */
    ERROR,
    /**
     * An event that might lead to an error.
     */
    WARN,
    /**
     * An event for informational purposes.
     */
    INFO,
    /**
     * A general debugging event.
     */
    DEBUG,
    /**
     * A fine-grained debug message, typically capturing the flow through the application.
     */
    TRACE,
    /**
     * All events should be logged.
     */
    ALL;
}
