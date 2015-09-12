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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.logging.ESLogger;

/**
 */
public class LoggingRunnable implements Runnable {

    private final Runnable runnable;

    private final ESLogger logger;

    public LoggingRunnable(ESLogger logger, Runnable runnable) {
        this.runnable = runnable;
        this.logger = logger;
    }

    @Override
    public void run() {
        try {
            runnable.run();
        } catch (Exception e) {
            logger.warn("failed to execute [{}]", e, runnable.toString());
        }
    }
}
