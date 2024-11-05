/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A wrapper around the {@link TaskManager} that creates a new {@link Task} to observe and control a {@link Flow}.  When the Flow is
 * initiated, a new Task is registered in the TaskManager that represents the Flow.  When the Flow ends, either through completion or error,
 * the Task will close in the TaskManager.  If a user manually closes the Task, it will cancel and close the underlying Flow.
 */
public class StreamingTaskManager {
    private final TaskManager taskManager;
    private final ThreadPool threadPool;

    @Inject
    public StreamingTaskManager(TransportService transportService, ThreadPool threadPool) {
        this(transportService.getTaskManager(), threadPool);
    }

    StreamingTaskManager(TaskManager taskManager, ThreadPool threadPool) {
        this.taskManager = taskManager;
        this.threadPool = threadPool;
    }

    public <E> Flow.Processor<E, E> create(String taskType, String taskAction) {
        return new TaskBackedProcessor<>(taskType, taskAction);
    }

    public static List<NamedWriteableRegistry.Entry> namedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(Task.Status.class, FlowTask.FlowStatus.NAME, FlowTask.FlowStatus.STREAM_READER));
    }

    private class TaskBackedProcessor<E> implements Flow.Processor<E, E> {
        private static final Logger log = LogManager.getLogger(TaskBackedProcessor.class);
        private final String taskType;
        private final String taskAction;
        private Flow.Subscriber<? super E> downstream;
        private Flow.Subscription upstream;
        private FlowTask task;
        private final AtomicBoolean isClosed = new AtomicBoolean(false);
        private final AtomicLong pendingRequests = new AtomicLong();

        private TaskBackedProcessor(String taskType, String taskAction) {
            this.taskType = taskType;
            this.taskAction = taskAction;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super E> subscriber) {
            if (downstream != null) {
                subscriber.onError(new IllegalStateException("Another subscriber is already subscribed."));
                return;
            }

            downstream = subscriber;
            openOrUpdateTask();
            downstream.onSubscribe(forwardingSubscription());
        }

        private void openOrUpdateTask() {
            if (task != null) {
                task.updateStatus(FlowTask.FlowStatus.CONNECTED);
            } else {
                try (var ignored = threadPool.getThreadContext().newTraceContext()) {
                    task = (FlowTask) taskManager.register(taskType, taskAction, new TaskAwareRequest() {
                        @Override
                        public void setParentTask(TaskId taskId) {
                            throw new UnsupportedOperationException("parent task id for streaming results shouldn't change");
                        }

                        @Override
                        public void setRequestId(long requestId) {
                            throw new UnsupportedOperationException("does not have request ID");
                        }

                        @Override
                        public TaskId getParentTask() {
                            return TaskId.EMPTY_TASK_ID;
                        }

                        @Override
                        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                            var flowTask = new FlowTask(id, type, action, "", parentTaskId, headers);
                            flowTask.addListener(TaskBackedProcessor.this::cancelTask);
                            return flowTask;
                        }
                    });
                }
            }
        }

        private void cancelTask() {
            if (isClosed.compareAndSet(false, true)) {
                if (upstream != null) {
                    upstream.cancel();
                }
                if (downstream != null) {
                    downstream.onComplete();
                }
            }
        }

        private Flow.Subscription forwardingSubscription() {
            return new Flow.Subscription() {
                @Override
                public void request(long n) {
                    if (isClosed.get()) {
                        downstream.onComplete(); // shouldn't happen, but reinforce that we're no longer listening
                    } else if (upstream != null) {
                        upstream.request(n);
                    } else {
                        pendingRequests.accumulateAndGet(n, Long::sum);
                    }
                }

                @Override
                public void cancel() {
                    finishTask();
                    if (upstream != null) {
                        upstream.cancel();
                    }
                }
            };
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (isClosed.get()) {
                subscription.cancel();
                return;
            }

            upstream = subscription;
            openOrUpdateTask();
            var currentRequestCount = pendingRequests.getAndSet(0);
            if (currentRequestCount != 0) {
                upstream.request(currentRequestCount);
            }
        }

        @Override
        public void onNext(E item) {
            if (isClosed.get()) {
                upstream.cancel(); // shouldn't happen, but reinforce that we're no longer listening
            } else {
                downstream.onNext(item);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            finishTask();
            if (downstream != null) {
                downstream.onError(throwable);
            } else {
                log.atDebug()
                    .withThrowable(throwable)
                    .log("onError was called before the downstream subscription, rethrowing to close listener.");
                throw new IllegalStateException("onError was called before the downstream subscription", throwable);
            }
        }

        @Override
        public void onComplete() {
            finishTask();
            if (downstream != null) {
                downstream.onComplete();
            }
        }

        private void finishTask() {
            if (isClosed.compareAndSet(false, true) && task != null) {
                taskManager.unregister(task);
            }
        }
    }
}
