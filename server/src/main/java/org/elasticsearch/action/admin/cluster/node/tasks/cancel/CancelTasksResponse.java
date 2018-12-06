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

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.List;


/**
 * Returns the list of tasks that were cancelled
 */
public class CancelTasksResponse extends ListTasksResponse {

    private static final ConstructingObjectParser<CancelTasksResponse, Void> PARSER =
        setupParser("cancel_tasks_response", CancelTasksResponse::new);

    public CancelTasksResponse(StreamInput in) throws IOException {
        super(in);
    }

    public CancelTasksResponse(List<TaskInfo> tasks, List<TaskOperationFailure> taskFailures, List<? extends ElasticsearchException>
        nodeFailures) {
        super(tasks, taskFailures, nodeFailures);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return super.toXContent(builder, params);
    }

    public static CancelTasksResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
