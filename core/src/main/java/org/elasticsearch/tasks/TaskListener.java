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

package org.elasticsearch.tasks;

/**
 * Listener for Task success or failure.
 */
public interface TaskListener<Response> {
    /**
     * Handle task response. This response may constitute a failure or a success
     * but it is up to the listener to make that decision.
     *
     * @param task
     *            the task being executed. May be null if the action doesn't
     *            create a task
     * @param response
     *            the response from the action that executed the task
     */
    void onResponse(Task task, Response response);

    /**
     * A failure caused by an exception at some phase of the task.
     *
     * @param task
     *            the task being executed. May be null if the action doesn't
     *            create a task
     * @param e
     *            the failure
     */
    void onFailure(Task task, Throwable e);

}
