/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class TestLoggers {
    private TestLoggers() {};

    /**
     * Set the level of the logger in tests, also allowing to set restricted loggers such as org.apache.http.
     *
     * If the new level is null, the logger will inherit it's level from its nearest ancestor with a non-null
     * level.
     */
    public static void setLevel(Logger logger, String level) {
        Loggers.setLevel(logger, level, List.of());
    }

    /**
     * Set the level of the logger in tests, also allowing to set restricted loggers such as org.apache.http.
     *
     * If the new level is null, the logger will inherit it's level from its nearest ancestor with a non-null
     * level.
     */
    public static void setLevel(Logger logger, Level level) {
        Loggers.setLevel(logger, level, List.of());
    }
}
