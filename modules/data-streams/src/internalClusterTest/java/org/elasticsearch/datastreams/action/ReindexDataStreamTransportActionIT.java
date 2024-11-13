/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamRequest;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamResponse;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ReindexDataStreamTransportActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    public void testStuff() {
        ReindexDataStreamRequest reindexDataStreamRequest = new ReindexDataStreamRequest("nonexistent_source");
        ReindexDataStreamResponse response = client().execute(
            new ActionType<ReindexDataStreamResponse>(ReindexDataStreamAction.NAME),
            reindexDataStreamRequest
        ).actionGet();
        String taskId = response.getTaskId();
        GetTaskRequest getTaskRequest = new GetTaskRequest();
        getTaskRequest.setPersistentTaskId(taskId);
        GetTaskResponse getTaskResponse = client().execute(TransportGetTaskAction.TYPE, getTaskRequest).actionGet();
        assertThat(Map.of(), equalTo(getTaskResponse.getTask().getErrorAsMap()));
    }
}
