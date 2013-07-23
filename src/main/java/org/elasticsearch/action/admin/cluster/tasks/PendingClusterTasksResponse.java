/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 */
public class PendingClusterTasksResponse extends ActionResponse implements Iterable<PendingClusterTask> {

    private List<PendingClusterTask> pendingTasks;

    PendingClusterTasksResponse() {
    }

    PendingClusterTasksResponse(List<PendingClusterTask> pendingTasks) {
        this.pendingTasks = pendingTasks;
    }

    public List<PendingClusterTask> pendingTasks() {
        return pendingTasks;
    }

    /**
     * The pending cluster tasks
     */
    public List<PendingClusterTask> getPendingTasks() {
        return pendingTasks();
    }

    @Override
    public Iterator<PendingClusterTask> iterator() {
        return pendingTasks.iterator();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        pendingTasks = new ArrayList<PendingClusterTask>(size);
        for (int i = 0; i < size; i++) {
            PendingClusterTask task = new PendingClusterTask();
            task.readFrom(in);
            pendingTasks.add(task);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(pendingTasks.size());
        for (PendingClusterTask task : pendingTasks) {
            task.writeTo(out);
        }
    }

}
