/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.spi;

import org.elasticsearch.logging.Logger;
import org.elasticsearch.logging.core.Appender;
import org.elasticsearch.logging.core.Layout;
import org.elasticsearch.logging.core.MockLogAppender;
import org.elasticsearch.logging.locator.LoggingSupportLocator;

import java.util.List;

public interface AppenderSupport {

    static AppenderSupport provider() {
        return LoggingSupportProvider.provider().appenderSupport();
    }

    void addAppender(final org.elasticsearch.logging.Logger logger, final org.elasticsearch.logging.core.Appender appender);

    void addAppender(final Logger logger, final MockLogAppender appender);

    void removeAppender(final Logger logger, final org.elasticsearch.logging.core.Appender appender);

    void removeAppender(final Logger logger, final MockLogAppender appender);

    Layout createECSLayout(String dataset);

//    RateLimitingFilter createRateLimitingFilter();

    Appender createMockLogAppender(List<MockLogAppender.LoggingExpectation> expectations) throws IllegalAccessException;
}
