/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.SuppressLoggerChecks;

/**
 * This class wraps both <code>HeaderWarningLogger</code> and <code>ThrottlingLogger</code>
 * which is a common use case across Elasticsearch
 */
public class ThrottlingAndHeaderWarningLogger {
    private final HeaderWarningLogger headerWarningLogger = new HeaderWarningLogger();
    private final ThrottlingLogger throttlingLogger;

    public ThrottlingAndHeaderWarningLogger(Logger loggerName) {
        this.throttlingLogger = new ThrottlingLogger(LogManager.getLogger(loggerName));
    }

    /**
     * Logs a message, adding a formatted warning message as a response header on the thread context.
     */
    @SuppressLoggerChecks(reason = "safely delegates to logger")
    public void logAndWarnOnHeader(String msg, Object... params) {
        HeaderWarningLogger.addWarning(msg, params);
        Message message = new ParameterizedMessage(msg, params);
        throttlingLogger.log(message);
    }

    /**
     * Adds a formatted warning message as a response header on the thread context, and logs a message if the associated key has
     * not recently been seen.
     *
     * @param key    the key used to determine if this message should be logged
     * @param msg    the message to log
     * @param params parameters to the message
     */
    @SuppressLoggerChecks(reason = "safely delegates to logger")
    public void throttleLogAndWarnOnHeader(final String key, final String msg, final Object... params) {
        HeaderWarningLogger.addWarning(msg, params);
        ESLogMessage message = new ESLogMessage(msg, params);
        throttlingLogger.throttleLog(key, message);
    }

    public void throttleLogAndWarnOnHeader(final String key, ESLogMessage message) {
        HeaderWarningLogger.addWarning(message.getMessagePattern(), message.getArguments());
        throttlingLogger.throttleLog(key, message);
    }

}
