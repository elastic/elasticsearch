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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * TODO wrapping logging this way limits the usage of %location. It will think this is used from that class.
 * <p>
 * This is a wrapper around a logger that allows to throttle log messages.
 * In order to throttle a key has to be used and throttling happens per each key combined with X-Opaque-Id.
 * X-Opaque-Id allows throttling per user. This value is set in ThreadContext from X-Opaque-Id HTTP header.
 * <p>
 * The throttling algorithm is relying on LRU set of keys which evicts entries when its size is &gt; 128.
 * When a log with a key is emitted, it won't be logged again until the set reaches size 128 and the key is removed from the set.
 *
 * @see HeaderWarning
 */
class ThrottlingLogger {

    // LRU set of keys used to determine if a message should be emitted to the logs
    private final Set<String> keys = Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<String, Boolean>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, Boolean> eldest) {
            return size() > 128;
        }
    }));

    private final Logger logger;

    ThrottlingLogger(Logger logger) {
        this.logger = logger;
    }

    void throttleLog(String key, Message message) {
        String xOpaqueId = HeaderWarning.getXOpaqueId();
        boolean shouldLog = keys.add(xOpaqueId + key);
        if (shouldLog) {
            log(message);
        }
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
