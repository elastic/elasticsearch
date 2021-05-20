/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

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

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public class AnalyzeIndexDiskUsageRequest extends BroadcastRequest<AnalyzeIndexDiskUsageRequest> {
    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, true);
    final boolean flush;

    public AnalyzeIndexDiskUsageRequest(String[] indices, IndicesOptions indicesOptions, boolean flush) {
        super(indices, indicesOptions);
        this.flush = flush;
    }

    public AnalyzeIndexDiskUsageRequest(StreamInput in) throws IOException {
        super(in);
        this.flush = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(flush);
    }

    @Override
    public void setParentTask(String parentTaskNode, long parentTaskId) {
        super.setParentTask(parentTaskNode, parentTaskId);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, AnalyzeIndexDiskUsageAction.NAME, type, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return AnalyzeIndexDiskUsageRequest.this.getDescription();
            }
        };
    }

    @Override
    public String getDescription() {
        return "analyze disk usage indices [" + String.join(",", indices) + "], flush [" + flush + "]";
    }
}
