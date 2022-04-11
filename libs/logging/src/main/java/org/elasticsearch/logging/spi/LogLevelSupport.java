/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.spi;

/**
 * the SPI for changing logger level. Used for slow logs (we want to make sure the level is TRACE)
 * and dynamic change of logger level via settings api
 */
public interface LogLevelSupport {

    static LogLevelSupport provider() {
        return LoggingSupportProvider.provider().logLevelSupport();
    }

    void setRootLoggerLevel(String level);

    void setRootLoggerLevel(org.elasticsearch.logging.Level level);

    /**
     * Set the level of the logger. If the new level is null, the logger will inherit it's level from its nearest ancestor with a non-null
     * level.
     */
    void setLevel(org.elasticsearch.logging.Logger logger, String level);

    void setLevel(org.elasticsearch.logging.Logger logger, org.elasticsearch.logging.Level level);

}
