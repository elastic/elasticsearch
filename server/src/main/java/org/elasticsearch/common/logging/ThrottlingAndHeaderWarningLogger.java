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

/**
 * This class wraps both <code>HeaderWarningLogger</code> and <code>ThrottlingLogger</code>
 * which is a common use case across Elasticsearch
 */
public class ThrottlingAndHeaderWarningLogger {
    private final HeaderWarningLogger headerWarningLogger = new HeaderWarningLogger();
    private final ThrottlingLogger throttlingLogger;

    public ThrottlingAndHeaderWarningLogger(String loggerName) {
        this.throttlingLogger = new ThrottlingLogger(LogManager.getLogger(loggerName));
    }

    /**
     * Logs a message, adding a formatted warning message as a response header on the thread context.
     */
    public void headerWarnAndLog(String msg, Object... params) {
        headerWarningLogger.log(msg, params);
        throttlingLogger.log(msg, params);
    }

    /**
     * Adds a formatted warning message as a response header on the thread context, and logs a message if the associated key has
     * not recently been seen.
     *
     * @param key    the key used to determine if this message should be logged
     * @param msg    the message to log
     * @param params parameters to the message
     */
    public void headerWarnAndThrottleLog(final String key, final String msg, final Object... params) {
        headerWarningLogger.log(msg, params);
        throttlingLogger.throttleLog(key, msg, params);
    }
}
