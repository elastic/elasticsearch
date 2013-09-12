/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.junit.listeners;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.junit.annotations.TestLogging;
import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link RunListener} that allows to change the log level for a specific test method.
 * When a test method is annotated with the {@link org.elasticsearch.junit.annotations.TestLogging} annotation, the level for the specified loggers
 * will be internally saved before the test method execution and overridden with the specified ones.
 * At the end of the test method execution the original loggers levels will be restored.
 *
 * Note: This class is not thread-safe. Given the static nature of the logging api, it assumes that tests
 * are never run concurrently in the same jvm. For the very same reason no synchronization has been implemented
 * regarding the save/restore process of the original loggers levels.
 */
public class LoggingListener extends RunListener {

    private Map<String, String> previousLoggingMap;

    @Override
    public void testStarted(Description description) throws Exception {
        TestLogging testLogging = description.getAnnotation(TestLogging.class);
        if (testLogging != null) {
            this.previousLoggingMap = new HashMap<String, String>();
            String[] loggersAndLevels = testLogging.value().split(",");
            for (String loggerAndLevel : loggersAndLevels) {
                String[] loggerAndLevelArray = loggerAndLevel.split(":");
                if (loggerAndLevelArray.length >=2) {
                    String loggerName = loggerAndLevelArray[0];
                    String level = loggerAndLevelArray[1];
                    ESLogger esLogger = resolveLogger(loggerName);
                    this.previousLoggingMap.put(loggerName, esLogger.getLevel());
                    esLogger.setLevel(level);
                }
            }
        }
    }

    @Override
    public void testFinished(Description description) throws Exception {
        if (this.previousLoggingMap != null) {
            for (Map.Entry<String, String> previousLogger : previousLoggingMap.entrySet()) {
                ESLogger esLogger = resolveLogger(previousLogger.getKey());
                esLogger.setLevel(previousLogger.getValue());
            }
            this.previousLoggingMap = null;
        }
    }

    private static ESLogger resolveLogger(String loggerName) {
        if (loggerName.equalsIgnoreCase("_root")) {
            return ESLoggerFactory.getRootLogger();
        }
        return Loggers.getLogger(loggerName);
    }
}
