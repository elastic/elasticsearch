/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.Set;

/**
 * An unsafe future. You should not need to use this for new code, rather you should be able to convert that code to be async
 * or use a clear hierarchy of thread pool executors around the future.
 * <p>
 * This future is unsafe, since it allows notifying the future on the same thread pool executor that it is being waited on. This
 * is a common deadlock scenario, since all threads may be waiting and thus no thread may be able to complete the future.
 * <p>
 * Note that the deadlock protection in {@link PlainActionFuture} is very weak. In general there's a risk of deadlock if there's any cycle
 * of threads which block/complete on each other's futures, or dispatch work to each other, but this is much harder to detect.
 */
@Deprecated(forRemoval = true)
public class UnsafePlainActionFuture<T> extends PlainActionFuture<T> {
    private final Set<String> unsafeExecutors;

    /**
     * Create a future which permits any of the given named executors to be used unsafely (i.e. used for both waiting for the future's
     * completion and completing the future).
     */
    public UnsafePlainActionFuture(String... unsafeExecutors) {
        assert unsafeExecutors.length > 0 : "use PlainActionFuture if there are no executors to use unsafely";
        this.unsafeExecutors = Set.of(unsafeExecutors);
    }

    @Override
    boolean allowedExecutors(Thread blockedThread, Thread completingThread) {
        return super.allowedExecutors(blockedThread, completingThread) || unsafeExecutors.contains(EsExecutors.executorName(blockedThread));
    }
}
