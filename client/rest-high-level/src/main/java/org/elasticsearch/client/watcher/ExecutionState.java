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

package org.elasticsearch.client.watcher;

import java.util.Locale;

public enum ExecutionState {

    // the condition of the watch was not met
    EXECUTION_NOT_NEEDED,

    // Execution has been throttled due to time-based throttling - this might only affect a single action though
    THROTTLED,

    // Execution has been throttled due to ack-based throttling/muting of an action - this might only affect a single action though
    ACKNOWLEDGED,

    // regular execution
    EXECUTED,

    // an error in the condition or the execution of the input
    FAILED,

    // a rejection due to a filled up threadpool
    THREADPOOL_REJECTION,

    // the execution was scheduled, but in between the watch was deleted
    NOT_EXECUTED_WATCH_MISSING,

    // even though the execution was scheduled, it was not executed, because the watch was already queued in the thread pool
    NOT_EXECUTED_ALREADY_QUEUED,

    // this can happen when a watch was executed, but not completely finished (the triggered watch entry was not deleted), and then
    // watcher is restarted (manually or due to host switch) - the triggered watch will be executed but the history entry already
    // exists
    EXECUTED_MULTIPLE_TIMES;

    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static ExecutionState resolve(String id) {
        return valueOf(id.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return id();
    }

}
