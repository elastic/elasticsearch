/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;

/**
 * A {@linkplain Client} that cancels tasks executed locally when the provided {@link HttpChannel}
 * is closed before completion.
 */
public class RestCancellableNodeClient extends FilterClient {
    private static final Map<HttpChannel, CloseListener> httpChannels = new ConcurrentHashMap<>();

    private final NodeClient client;
    private final HttpChannel httpChannel;

    public RestCancellableNodeClient(NodeClient client, HttpChannel httpChannel) {
        super(client);
        this.client = client;
        this.httpChannel = httpChannel;
    }

    /**
     * Returns the number of channels tracked globally.
     */
    public static int getNumChannels() {
        return httpChannels.size();
    }

    /**
     * Returns the number of tasks tracked globally.
     */
    static int getNumTasks() {
        return httpChannels.values().stream().mapToInt(CloseListener::getNumTasks).sum();
    }

    /**
     * Returns the number of tasks tracked by the provided {@link HttpChannel}.
     */
    static int getNumTasks(HttpChannel channel) {
        CloseListener listener = httpChannels.get(channel);
        return listener == null ? 0 : listener.getNumTasks();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        CloseListener closeListener = httpChannels.computeIfAbsent(httpChannel, channel -> new CloseListener());
        TaskHolder taskHolder = new TaskHolder();
        Task task = client.executeLocally(action, request, new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                try {
                    closeListener.unregisterTask(taskHolder);
                } finally {
                    listener.onResponse(response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    closeListener.unregisterTask(taskHolder);
                } finally {
                    listener.onFailure(e);
                }
            }
        });
        assert task instanceof CancellableTask : action.name() + " is not cancellable";
        final TaskId taskId = new TaskId(client.getLocalNodeId(), task.getId());
        closeListener.registerTask(taskHolder, taskId);
        closeListener.maybeRegisterChannel(httpChannel);
    }

    private void cancelTask(TaskId taskId) {
        CancelTasksRequest req = new CancelTasksRequest().setTargetTaskId(taskId).setReason("http channel [" + httpChannel + "] closed");
        // force the origin to execute the cancellation as a system user
        new OriginSettingClient(client, TASKS_ORIGIN).admin().cluster().cancelTasks(req, ActionListener.noop());
    }

    private class CloseListener implements ActionListener<Void> {
        private final AtomicReference<HttpChannel> channel = new AtomicReference<>();
        private final Set<TaskId> tasks = new HashSet<>();

        CloseListener() {}

        synchronized int getNumTasks() {
            return tasks.size();
        }

        void maybeRegisterChannel(HttpChannel httpChannel) {
            if (channel.compareAndSet(null, httpChannel)) {
                // In case the channel is already closed when we register the listener, the listener will be immediately executed which will
                // remove the channel from the map straight-away. That is why we first create the CloseListener and later we associate it
                // with the channel. This guarantees that the close listener is already in the map when it gets registered to its
                // corresponding channel, hence it is always found in the map when it gets invoked if the channel gets closed.
                httpChannel.addCloseListener(this);
            }
        }

        synchronized void registerTask(TaskHolder taskHolder, TaskId taskId) {
            taskHolder.taskId = taskId;
            if (taskHolder.completed == false) {
                this.tasks.add(taskId);
            }
        }

        synchronized void unregisterTask(TaskHolder taskHolder) {
            if (taskHolder.taskId != null) {
                this.tasks.remove(taskHolder.taskId);
            }
            taskHolder.completed = true;
        }

        @Override
        public void onResponse(Void aVoid) {
            final HttpChannel httpChannel = channel.get();
            assert httpChannel != null : "channel not registered";
            // when the channel gets closed it won't be reused: we can remove it from the map and forget about it.
            CloseListener closeListener = httpChannels.remove(httpChannel);
            assert closeListener != null : "channel not found in the map of tracked channels";
            final List<TaskId> toCancel;
            synchronized (this) {
                toCancel = new ArrayList<>(tasks);
                tasks.clear();
            }
            for (TaskId taskId : toCancel) {
                cancelTask(taskId);
            }
        }

        @Override
        public void onFailure(Exception e) {
            onResponse(null);
        }
    }

    private static class TaskHolder {
        private TaskId taskId;
        private boolean completed = false;
    }
}
