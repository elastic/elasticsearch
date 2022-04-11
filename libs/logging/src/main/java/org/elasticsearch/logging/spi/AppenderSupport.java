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

import java.util.List;

//more low level stuff. we can possibly limit the scope of the export
public interface AppenderSupport {

    static AppenderSupport provider() {
        return LoggingSupportProvider.provider().appenderSupport();
    }

    void addAppender(org.elasticsearch.logging.Logger logger, org.elasticsearch.logging.core.Appender appender);

    void addAppender(Logger logger, MockLogAppender appender);

    void removeAppender(Logger logger, org.elasticsearch.logging.core.Appender appender);

    void removeAppender(Logger logger, MockLogAppender appender);

    Layout createECSLayout(String dataset);

    Appender createMockLogAppender(List<MockLogAppender.LoggingExpectation> expectations) throws IllegalAccessException;
}
