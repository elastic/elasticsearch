/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;

import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * A utility that allows chained (serial) execution of a number of tasks
 * in async manner.
 */
public class ChainTaskExecutor {

    public interface ChainTask {
        void run(ActionListener<Void> listener);
    }

    private final ExecutorService executorService;
    private final boolean shortCircuit;
    private final LinkedList<ChainTask> tasks = new LinkedList<>();

    public ChainTaskExecutor(ExecutorService executorService, boolean shortCircuit) {
        this.executorService = Objects.requireNonNull(executorService);
        this.shortCircuit = shortCircuit;
    }

    public synchronized void add(ChainTask task) {
        tasks.add(task);
    }

    public synchronized void execute(ActionListener<Void> listener) {
        if (tasks.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        ChainTask task = tasks.pop();
        executorService.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (shortCircuit) {
                    listener.onFailure(e);
                } else {
                    execute(listener);
                }
            }

            @Override
            protected void doRun() {
                task.run(ActionListener.wrap(nullValue -> execute(listener), this::onFailure));
            }
        });
    }
}
