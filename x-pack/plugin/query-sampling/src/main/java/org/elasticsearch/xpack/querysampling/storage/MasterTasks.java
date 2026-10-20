/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Runs the periodic jobs that one node is enough for, on the elected master. Every node has the plugin and the others
 * would only repeat the same work, so the jobs are scheduled when this node becomes master and cancelled when it stops
 * being it: nothing is scheduled on the other nodes.
 */
public final class MasterTasks implements LocalNodeMasterListener {

    /** A job and how long to wait between the end of one run and the start of the next. */
    public record Task(Runnable job, TimeValue interval) {}

    private final ThreadPool threadPool;
    private final Executor executor;
    private final List<Task> tasks;
    private final List<Scheduler.Cancellable> scheduled = new ArrayList<>(); // guarded by this

    public MasterTasks(ThreadPool threadPool, Executor executor, List<Task> tasks) {
        this.threadPool = threadPool;
        this.executor = executor;
        this.tasks = List.copyOf(tasks);
    }

    @Override
    public synchronized void onMaster() {
        if (scheduled.isEmpty() == false) {
            return;
        }
        for (Task task : tasks) {
            scheduled.add(threadPool.scheduleWithFixedDelay(task.job(), task.interval(), executor));
        }
    }

    @Override
    public synchronized void offMaster() {
        scheduled.forEach(Scheduler.Cancellable::cancel);
        scheduled.clear();
    }
}
