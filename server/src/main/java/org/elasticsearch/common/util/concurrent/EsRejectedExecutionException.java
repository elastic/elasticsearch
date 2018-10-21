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

import java.util.concurrent.RejectedExecutionException;

public class EsRejectedExecutionException extends RejectedExecutionException {

    private final boolean isExecutorShutdown;

    public EsRejectedExecutionException(String message, boolean isExecutorShutdown) {
        super(message);
        this.isExecutorShutdown = isExecutorShutdown;
    }

    public EsRejectedExecutionException(String message) {
        this(message, false);
    }

    public EsRejectedExecutionException() {
        this(null, false);
    }

    /**
     * Checks if the thread pool that rejected the execution was terminated
     * shortly after the rejection. Its possible that this returns false and the
     * thread pool has since been terminated but if this returns false then the
     * termination wasn't a factor in this rejection. Conversely if this returns
     * true the shutdown was probably a factor in this rejection but might have
     * been triggered just after the action rejection.
     */
    public boolean isExecutorShutdown() {
        return isExecutorShutdown;
    }

}
