/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DeprecationLoggerTests extends ESTestCase {

    public void testMultipleSlowLoggersUseSingleLog4jLogger() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);

        DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DeprecationLoggerTests.class);
        int numberOfLoggersBefore = context.getLoggers().size();

        class LoggerTest{
        }
        DeprecationLogger deprecationLogger2 = DeprecationLogger.getLogger(LoggerTest.class);

        context = (LoggerContext) LogManager.getContext(false);
        int numberOfLoggersAfter = context.getLoggers().size();

        assertThat(numberOfLoggersAfter, equalTo(numberOfLoggersBefore + 1));
    }
}
