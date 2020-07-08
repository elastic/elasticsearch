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
 * This class writes deprecation messages to a specified logger at <code>WARN</code> level.
 * <p>
 * TODO wrapping logging this way limits the usage of %location. It will think this is used from that class.
 */
class WarningLogger {
    private final Logger logger;

    WarningLogger(Logger logger) {
        this.logger = logger;
    }

    public void log(ESLogMessage message) {
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
