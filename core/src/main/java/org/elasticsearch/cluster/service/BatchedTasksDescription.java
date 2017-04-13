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

import java.util.List;

public interface BatchedTasksDescription<T> {
    /**
     * Builds a concise description of a list of tasks (to be used in logging etc.).
     *
     * This method can be called multiple times with different lists before execution.
     * This allows groupd task description but the submitting source.
     */
    default String describeTasks(List<T> tasks) {
        return tasks.stream().map(T::toString).reduce((s1, s2) -> {
            if (s1.isEmpty()) {
                return s2;
            } else if (s2.isEmpty()) {
                return s1;
            } else {
                return s1 + ", " + s2;
            }
        }).orElse("");
    }
}
