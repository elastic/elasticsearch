/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl.provider;

import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.logging.impl.Loggers;
import org.elasticsearch.logging.impl.Util;
import org.elasticsearch.logging.spi.LogLevelSupport;

public class LogLevelSupportImpl implements LogLevelSupport {
    @Override
    public void setRootLoggerLevel(String level) {
        // Loggers.setLevelImpl(LogManager.getRootLogger(), level);

    }

    @Override
    public void setRootLoggerLevel(Level level) {
        // Loggers.setLevelImpl(LogManager.getRootLogger(), Util.log4jLevel(level));

    }

    @Override
    public void setLevel(Logger logger, String level) {
        // Loggers.setLevelImpl(Util.log4jLogger(logger), level);

    }

    @Override
    public void setLevel(Logger logger, Level level) {
        Loggers.setLevelImpl(Util.log4jLogger(logger), Util.log4jLevel(level));

    }

}
