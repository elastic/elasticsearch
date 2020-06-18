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

import org.apache.logging.log4j.Logger;

/**
 * This class wraps both <code>HeaderWarningLogger</code> and <code>ThrottlingLogger</code>
 * which is a common use case across Elasticsearch
 */
class ThrottlingAndHeaderWarningLogger {
    private final ThrottlingLogger throttlingLogger;

    ThrottlingAndHeaderWarningLogger(Logger logger) {
        this.throttlingLogger = new ThrottlingLogger(logger);
    }

    /**
     * Adds a formatted warning message as a response header on the thread context, and logs a message if the associated key has
     * not recently been seen.
     *
     * @param key     the key used to determine if this message should be logged
     * @param message the message to log
     */
    void throttleLogAndAddWarning(final String key, ESLogMessage message) {
        String messagePattern = message.getMessagePattern();
        Object[] arguments = message.getArguments();
        HeaderWarning.addWarning(messagePattern, arguments);
        throttlingLogger.throttleLog(key, message);
    }

}
