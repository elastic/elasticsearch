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
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This class composes {@link HeaderWarning}, {@link DeprecationIndexingService} and {@link Logger},
 * in order to apply a single message to multiple destination.
 * <p>
 * Logging and indexing are throttled in order to avoid filling the destination with duplicates.
 * Throttling is implemented using a mandatory per-message key combined with any <code>X-Opaque-Id</code>
 * HTTP header value. This header allows throttling per user. This value is set in {@link ThreadContext}.
 * <p>
 * TODO wrapping logging this way limits the usage of %location. It will think this is used from that class.
 */
class ThrottlingAndHeaderWarningLogger {
    private final Logger logger;
    private final RateLimiter rateLimiter;
    private final DeprecationIndexingService indexingService;

    ThrottlingAndHeaderWarningLogger(Logger logger, DeprecationIndexingService indexingService) {
        this.logger = logger;
        this.rateLimiter = new RateLimiter();
        this.indexingService = indexingService;
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

        String xOpaqueId = HeaderWarning.getXOpaqueId();
        this.rateLimiter.limit(xOpaqueId + key, () -> {
            log(message);
            if (indexingService != null) {
                indexingService.writeMessage(key, messagePattern, xOpaqueId, arguments);
            }
        });
    }

    private void log(Message message) {
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
