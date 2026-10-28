/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.internal.LevelUtil;
import org.elasticsearch.logging.Level;

/**
 * Writer that just uses one of the standard log4j loggers.
 * TODO: Convert this class to use ES Logger API.
 */
public class Log4jWriter implements ActivityLogWriter {
    public static final ActivityLogWriterProvider PROVIDER = Log4jWriter::new;

    private final Logger logger;

    public Log4jWriter(Logger logger) {
        this.logger = logger;
    }

    public Log4jWriter(String loggerName) {
        this(LogManager.getLogger(loggerName));
    }

    @Override
    public void write(Level level, ESLogMessage message) {
        logger.log(LevelUtil.log4jLevel(level), message);
    }

}
