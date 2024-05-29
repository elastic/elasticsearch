/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.test.ESTestCase;

import java.util.NavigableMap;

import static org.hamcrest.Matchers.equalTo;

public class TestLoggersTests extends ESTestCase {

    public void testRunWithLoggersRestored() {
        NavigableMap<String, Level> levels = TestLoggers.getLogLevels();

        TestLoggers.runWithLoggersRestored(() -> {
            Loggers.setLevel(LogManager.getRootLogger(), Level.WARN);
            Loggers.setLevel(LogManager.getLogger(TestLoggers.class), Level.DEBUG);
        });

        assertThat(TestLoggers.getLogLevels(), equalTo(levels));
    }
}
