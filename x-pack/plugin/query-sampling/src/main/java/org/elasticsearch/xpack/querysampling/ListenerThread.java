/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountAwareThreadedActionListener;
import org.elasticsearch.core.RefCounted;

import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 * A call is completed by whichever thread has the last of its answers, which can be a network thread. Reading ten thousand
 * records or building as many documents must not be done by one, so the readers and the writers of the plugin are given
 * their calls through this, which completes the listener on an executor. A response such as a search response is released
 * by the thread that completes the call as soon as the listener returns, so it is held until the executor has handled it.
 */
public final class ListenerThread {

    private ListenerThread() {}

    public static <Request, Response extends RefCounted> BiConsumer<Request, ActionListener<Response>> on(
        Executor executor,
        BiConsumer<Request, ActionListener<Response>> call
    ) {
        return (request, listener) -> call.accept(request, new RefCountAwareThreadedActionListener<>(executor, listener));
    }
}
