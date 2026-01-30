/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.MapMessage;
import org.elasticsearch.common.logging.internal.LevelUtil;

/**
 * Writer that just uses one of the standard log4j loggers.
 */
public class Log4jActionWriter implements ActionLogWriter {

    private final Logger logger;

    public Log4jActionWriter(Logger logger) {
        this.logger = logger;
    }

    public Log4jActionWriter(String loggerName) {
        this(LogManager.getLogger(loggerName));
    }

    public void write(ActionLogMessage message) {
        logger.log(LevelUtil.log4jLevel(message.level()), new MapMessage<>(message));
    }

}
