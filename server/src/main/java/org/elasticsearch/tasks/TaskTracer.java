/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.tracing.Tracer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TaskTracer {

    private static final Logger logger = LogManager.getLogger();

    private final List<Tracer> tracers = new CopyOnWriteArrayList<>();

    public void addTracer(Tracer tracer) {
        if (tracer != null) {
            tracers.add(tracer);
        }
    }

    public void onTaskRegistered(Task task) {
        for (Tracer tracer : tracers) {
            try {
                tracer.onTraceStarted(task);
            } catch (Exception e) {
                assert false : e;
                logger.warn(
                    new ParameterizedMessage(
                        "task tracing listener [{}] failed on registration of task [{}][{}]",
                        tracer,
                        task.getId(),
                        task.getAction()
                    ),
                    e
                );
            }
        }
    }

    public void onTaskUnregistered(Task task) {
        for (Tracer tracer : tracers) {
            try {
                tracer.onTraceStopped(task);
            } catch (Exception e) {
                assert false : e;
                logger.warn(
                    new ParameterizedMessage(
                        "task tracing listener [{}] failed on unregistration of task [{}][{}]",
                        tracer,
                        task.getId(),
                        task.getAction()
                    ),
                    e
                );
            }
        }
    }
}
