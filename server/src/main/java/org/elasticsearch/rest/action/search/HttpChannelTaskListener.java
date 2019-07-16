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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskListener;

/**
 * {@link TaskListener} implementation that supports interaction with {@link HttpChannelTaskHandler} so that the link between the http
 * channel and its corresponding task is removed once the request is completed.
 */
public final class HttpChannelTaskListener<Response> implements TaskListener<Response> {
    private final HttpChannelTaskHandler httpChannelTaskHandler;
    private final ActionListener<Response> delegateListener;
    private final HttpChannel httpChannel;
    private final String localNodeId;

    HttpChannelTaskListener(HttpChannelTaskHandler httpChannelTaskHandler, ActionListener<Response> delegateListener,
                            HttpChannel httpChannel, String localNodeId) {
        this.httpChannelTaskHandler = httpChannelTaskHandler;
        this.delegateListener = delegateListener;
        this.httpChannel = httpChannel;
        this.localNodeId = localNodeId;
    }

    //TODO verify that the order in which listeners get notified is guaranteed: this action listener needs to be notified
    // BEFORE the on close listener (in case the underlying connection gets closed at completion), otherwise we end up
    // cancelling tasks for requests that are about to return a response.
    @Override
    public void onResponse(Task task, Response response) {
        TaskId taskId = new TaskId(localNodeId, task.getId());
        //TODO this may be called even before the channel is linked with the task
        httpChannelTaskHandler.unlinkChannelFromTask(httpChannel, taskId);
        delegateListener.onResponse(response);
    }

    @Override
    public void onFailure(Task task, Throwable e) {
        TaskId taskId = new TaskId(localNodeId, task.getId());
        httpChannelTaskHandler.unlinkChannelFromTask(httpChannel, taskId);
        if (e instanceof Exception) {
            delegateListener.onFailure(new RuntimeException(e));
        }
    }
}
