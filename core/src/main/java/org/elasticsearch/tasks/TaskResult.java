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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskInfo;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * Represents the result or failure of a running task
 */
public class TaskResult {

    private final BytesReference result;

    private final TaskId taskId;

    public TaskResult(TaskInfo taskInfo, Throwable e) throws IOException {
        ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE);
        builder.startObject();
        {
            builder.startObject("task");
            {
                taskInfo.toXContent(builder, params);
            }
            builder.endObject();
            builder.startObject("error");
            {
                ElasticsearchException.toXContent(builder, params, e);
            }
            builder.endObject();
        }
        builder.endObject();
        result = builder.bytes();
        taskId = taskInfo.getTaskId();
    }

    public TaskResult(TaskInfo taskInfo, ToXContent toXContent) throws IOException {
        ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE);
        builder.startObject();
        {
            builder.startObject("task");
            {
                taskInfo.toXContent(builder, params);
            }
            builder.endObject();
            builder.startObject("result");
            {
                toXContent.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        result = builder.bytes();
        taskId = taskInfo.getTaskId();
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public BytesReference getResult() {
        return result;
    }
}
