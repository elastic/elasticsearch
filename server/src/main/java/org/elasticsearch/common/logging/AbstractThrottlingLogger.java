/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A logger that logs notices.
 */
public abstract class AbstractThrottlingLogger extends AbstractHeaderWarningLogger{

    /**
     * Logs a message, adding a formatted warning message as a response header on the thread context.
     */
    public void log(String msg, Object... params) {
        log(THREAD_CONTEXT, msg, params);
    }

    // LRU set of keys used to determine if a message should be emitted to the logs
    private final Set<String> keys = Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<String, Boolean>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, Boolean> eldest) {
            return size() > 128;
        }
    }));

    /**
     * Adds a formatted warning message as a response header on the thread context, and logs a deprecation message if the associated key has
     * not recently been seen.
     *
     * @param key    the key used to determine if this deprecation should be logged
     * @param msg    the message to log
     * @param params parameters to the message
     */
    public void headerWarningAndMaybeLog(final String key, final String msg, final Object... params) {
        String xOpaqueId = getXOpaqueId(THREAD_CONTEXT);
        boolean shouldLog = keys.add(xOpaqueId + key);
        log(THREAD_CONTEXT, msg, shouldLog, params);
    }

    /**
     * Logs a deprecated message to the deprecation log, as well as to the local {@link ThreadContext}.
     *
     * @param threadContexts The node's {@link ThreadContext} (outside of concurrent tests, this should only ever have one context).
     * @param message The deprecation message.
     * @param params The parameters used to fill in the message, if any exist.
     */
    void log(final Set<ThreadContext> threadContexts, final String message, final Object... params) {
        log(threadContexts, message, true, params);
    }

    void log(final Set<ThreadContext> threadContexts, final String message, final boolean shouldLog, final Object... params) {
        addWarningToHeaders(threadContexts, message, params);

        if (shouldLog) {
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @SuppressLoggerChecks(reason = "safely delegates to logger")
                @Override
                public Void run() {
                    /**
                     * There should be only one threadContext (in prod env), @see DeprecationLogger#setThreadContext
                     */
                    String opaqueId = getXOpaqueId(threadContexts);

                    logger().warn(DeprecatedMessage.of(opaqueId, message, params));
                    return null;
                }
            });
        }
    }

    public String getXOpaqueId(Set<ThreadContext> threadContexts) {
        return threadContexts.stream()
                             .filter(t -> t.getHeader(Task.X_OPAQUE_ID) != null)
                             .findFirst()
                             .map(t -> t.getHeader(Task.X_OPAQUE_ID))
                             .orElse("");
    }

    abstract protected Logger logger();

}
