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

package org.elasticsearch.cluster.service;

/**
 * An exception thrown is a cluster state update task is submitted for an executor and that task
 * already exists in the executor's task queue.
 */
public class DuplicateClusterStateUpdateTaskException extends IllegalArgumentException {

    // classes that extend from Throwable can not be generic so we are weakly-typed here
    private final Object task;

    DuplicateClusterStateUpdateTaskException(final String message, final Object task) {
        super(message);
        this.task = task;
    }

    /**
     * The queued task object.
     *
     * @return a reference to the queued task.
     */
    public Object task() {
        return task;
    }

}
