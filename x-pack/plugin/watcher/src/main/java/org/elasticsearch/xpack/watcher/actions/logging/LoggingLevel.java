/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.logging;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.SuppressLoggerChecks;

import java.util.Locale;

public enum LoggingLevel {

    ERROR() {
        @Override
        @SuppressLoggerChecks(reason = "logger delegation")
        void log(Logger logger, String text) {
            logger.error(text);
        }
    },
    WARN() {
        @Override
        @SuppressLoggerChecks(reason = "logger delegation")
        void log(Logger logger, String text) {
            logger.warn(text);
        }
    },
    INFO() {
        @Override
        @SuppressLoggerChecks(reason = "logger delegation")
        void log(Logger logger, String text) {
            logger.info(text);
        }
    },
    DEBUG() {
        @Override
        @SuppressLoggerChecks(reason = "logger delegation")
        void log(Logger logger, String text) {
            logger.debug(text);
        }
    },
    TRACE() {
        @Override
        @SuppressLoggerChecks(reason = "logger delegation")
        void log(Logger logger, String text) {
            logger.trace(text);
        }
    };

    abstract void log(Logger logger, String text);

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}
