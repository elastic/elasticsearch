/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.api.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.elasticsearch.logging.internal.LogEventImpl;
import org.elasticsearch.logging.internal.Util;

import java.io.Serializable;

public class AppenderUtils {

    private AppenderUtils() {
    }

    public static void addAppender(final org.elasticsearch.logging.Logger logger, final org.elasticsearch.logging.api.core.Appender  appender) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        org.apache.logging.log4j.core.Appender appender1 = createLog4jAdapter(appender);
        appender1.start();

        config.addAppender(appender1);
        LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
        if (logger.getName().equals(loggerConfig.getName()) == false) {
            loggerConfig = new LoggerConfig(logger.getName(), Util.log4jLevel(logger.getLevel()), true);
            config.addLogger(logger.getName(), loggerConfig);
        }
        loggerConfig.addAppender(appender1, null, null);
        ctx.updateLoggers();
    }

    @SuppressWarnings("unchecked")
    private static Appender createLog4jAdapter(org.elasticsearch.logging.api.core.Appender appender) {
        return new AbstractAppender(appender.name(), (Filter) appender.filter(),
            (Layout<? extends Serializable>) appender.layout(),
            false, Property.EMPTY_ARRAY ){

            @Override
            public void append(LogEvent event) {
                appender.append(new LogEventImpl(event));
            }


        };
    }

    public static void removeAppender(final org.elasticsearch.logging.Logger logger, final org.elasticsearch.logging.api.core.Appender  appender) {

    }

}
