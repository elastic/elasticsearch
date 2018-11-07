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

public enum ExecutionPhase {

    // awaiting execution of the watch
    AWAITS_EXECUTION(false),
    // initial phase, watch execution has started, but the input is not yet processed
    STARTED(false),
    // input is being executed
    INPUT(false),
    // condition phase is being executed
    CONDITION(false),
    // transform phase (optional, depends if a global transform was configured in the watch)
    WATCH_TRANSFORM(false),
    // actions phase, all actions, including specific action transforms
    ACTIONS(false),
    // missing watch, failed execution of input/condition/transform,
    ABORTED(true),
    // successful run
    FINISHED(true);

    private final boolean sealed;

    ExecutionPhase(boolean sealed) {
        this.sealed = sealed;
    }

    public boolean sealed() {
        return sealed;
    }
}
