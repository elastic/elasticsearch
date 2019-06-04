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
package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.routing.allocation.AllocationService;

import java.util.List;

public class DeferredRerouteTaskExecutor implements ClusterStateTaskExecutor<DeferredRerouteTaskExecutor.Task> {
    private AllocationService allocationService;

    public DeferredRerouteTaskExecutor(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

    @Override
    public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> tasks) throws Exception {
        final ClusterTasksResult.Builder<Task> resultBuilder = new ClusterTasksResult.Builder<>();
        return resultBuilder.successes(tasks).build(allocationService.reroute(currentState, "deferred reroute"));
    }

    @Override
    public String describeTasks(List<Task> tasks) {
        return ""; // the tasks are all the same
    }

    public static class Task {
    }
}
