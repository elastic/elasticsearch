/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.telemetry;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.tracing.Traceable;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public record TransformTraceEvents(
    Tracer tracer,
    Map<String, Object> attributes,
    Supplier<ThreadContext> threadContext,
    String traceName,
    AtomicLong checkpoint
) implements AsyncTwoPhaseIndexer.EventHook {

    public TransformTraceEvents(
        Tracer tracer,
        Supplier<ThreadContext> threadContext,
        String traceName,
        Map<String, Object> attributes,
        long checkpoint
    ) {
        this(tracer, attributes, threadContext, traceName, new AtomicLong(checkpoint));
    }

    @Override
    public void onEvent(Event event) {
        switch (event) {
            case START -> tracer.startTrace(threadContext.get(), spanId(checkpoint.get()), traceName, attributes);
            case FINISH -> tracer.stopTrace(spanId(checkpoint.getAndIncrement()));
        }
    }

    private Traceable spanId(long suffix) {
        return () -> traceName + "-" + suffix;
    }

    @Override
    public void onError(Throwable t) {
        tracer.addError(spanId(checkpoint.get()), t);
    }

    public static TransformTraceEvents create(Tracer tracer, ThreadPool threadPool, TransformTask task, long checkpoint) {
        return new TransformTraceEvents(
            tracer,
            threadPool::getThreadContext,
            "transform/" + task.getTransformId(),
            Map.of(
                Tracer.AttributeKeys.TASK_ID,
                task.getId(),
                Tracer.AttributeKeys.PARENT_TASK_ID,
                task.getParentTaskId().toString(),
                "Transform",
                task.getTransformId()
            ),
            checkpoint
        );
    }
}
