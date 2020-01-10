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
package org.elasticsearch.persistent;

/**
 * A dynamic persistent tasks executor is notified of updates to params of tasks.
 * @param <T> Type of allocated task
 * @param <P> TYpe of params
 */
public abstract class DynamicPersistentTasksExecutor<T extends AllocatedPersistentTask, P extends PersistentTaskParams>
    extends PersistentTasksExecutor<P> {
    public DynamicPersistentTasksExecutor(String taskName, String executor) {
        super(taskName, executor);
    }

    /**
     * Handle incoming params update.
     * @param task the task for which params where updated
     * @param newParams the new params.
     */
    protected abstract void paramsUpdated(T task, P newParams);
}
