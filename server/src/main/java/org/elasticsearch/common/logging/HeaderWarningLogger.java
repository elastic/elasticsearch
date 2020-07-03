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
import org.elasticsearch.common.SuppressLoggerChecks;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This class composes {@link HeaderWarning} and {@link Logger}, in order to both log a message
 * and add a header to an API response.
 * <p>
 * TODO wrapping logging this way limits the usage of %location. It will think this is used from that class.
 */
class HeaderWarningLogger implements DeprecatedLogHandler {
    private final Logger logger;

    HeaderWarningLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Logs a deprecation message and adds a formatted warning message as a response
     * header on the thread context.
     */
    @Override
    public void log(final String key, String xOpaqueId, ESLogMessage message) {
        String messagePattern = message.getMessagePattern();
        Object[] arguments = message.getArguments();
        HeaderWarning.addWarning(messagePattern, arguments);

        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @SuppressLoggerChecks(reason = "safely delegates to logger")
            @Override
            public Void run() {
                logger.warn(message);
                return null;
            }
        });
    }
}
