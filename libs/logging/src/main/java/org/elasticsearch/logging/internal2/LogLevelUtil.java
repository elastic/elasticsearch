/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal2;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.logging.internal.Loggers;
import org.elasticsearch.logging.internal.Util;

public class LogLevelUtil {

    private LogLevelUtil() {}

    public static void setRootLoggerLevel(String level) {
        // Loggers.setLevelImpl(LogManager.getRootLogger(), level);
    }

    public static void setRootLoggerLevel(org.elasticsearch.logging.Level level) {
        Loggers.setLevelImpl(LogManager.getRootLogger(), Util.log4jLevel(level));
    }

    /**
     * Set the level of the logger. If the new level is null, the logger will inherit it's level from its nearest ancestor with a non-null
     * level.
     */
    public static void setLevel(org.elasticsearch.logging.Logger logger, String level) {
        // Loggers.setLevelImpl(Util.log4jLogger(logger), level);
    }

    public static void setLevel(org.elasticsearch.logging.Logger logger, org.elasticsearch.logging.Level level) {
        Loggers.setLevelImpl(Util.log4jLogger(logger), Util.log4jLevel(level));
    }

}
