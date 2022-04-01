/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl.provider;

import org.elasticsearch.logging.spi.AppenderSupport;
import org.elasticsearch.logging.spi.LogLevelSupport;
import org.elasticsearch.logging.spi.LogManagerFactory;
import org.elasticsearch.logging.spi.LoggingBootstrapSupport;
import org.elasticsearch.logging.spi.LoggingSupportProvider;
import org.elasticsearch.logging.spi.MessageFactory;
import org.elasticsearch.logging.spi.StringBuildersSupport;

public class LoggingSupportProviderImpl implements LoggingSupportProvider {
    private final AppenderSupport appenderSupport = new AppenderSupportImpl();
    private final LoggingBootstrapSupport loggingBootstrapSupport = new Log4JBootstrapSupportImpl();
    private final LogManagerFactory logManagerFactory = new Log4jLogManagerFactory();
    private final MessageFactory messageFactory = new Log4JMessageFactoryImpl();
    private final LogLevelSupport logLevelSupport = new LogLevelSupportImpl();
    private final StringBuildersSupport stringBuildersSupport = new StringBuildersSupportImpl();

    @Override
    public AppenderSupport appenderSupport() {
        return appenderSupport;
    }

    @Override
    public LoggingBootstrapSupport loggingBootstrapSupport() {
        return loggingBootstrapSupport;
    }

    @Override
    public LogLevelSupport logLevelSupport() {
        return logLevelSupport;
    }

    @Override
    public LogManagerFactory logManagerFactory() {
        return logManagerFactory;
    }

    @Override
    public MessageFactory messageFactory() {
        return messageFactory;
    }

    @Override
    public StringBuildersSupport stringBuildersSupport() {
        return stringBuildersSupport;
    }
}
