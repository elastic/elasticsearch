/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.Objects;

/**
 * An unsafe future. You should not need to use this for new code, rather you should be able to convert that code to be async
 * or use a clear hierarchy of thread pool executors around the future.
 *
 * This future is unsafe, since it allows notifying the future on the same thread pool executor that it is being waited on. This
 * is a common deadlock scenario, since all threads may be waiting and thus no thread may be able to complete the future.
 */
@Deprecated(forRemoval = true)
public class UnsafePlainActionFuture<T> extends PlainActionFuture<T> {

    private final String unsafeExecutor;
    private final String unsafeExecutor2;

    public UnsafePlainActionFuture(String unsafeExecutor) {
        this(unsafeExecutor, null);
    }

    public UnsafePlainActionFuture(String unsafeExecutor, String unsafeExecutor2) {
        Objects.requireNonNull(unsafeExecutor);
        this.unsafeExecutor = unsafeExecutor;
        this.unsafeExecutor2 = unsafeExecutor2;
    }

    @Override
    boolean allowedExecutors(Thread thread1, Thread thread2) {
        return super.allowedExecutors(thread1, thread2)
            || unsafeExecutor.equals(EsExecutors.executorName(thread1))
            || unsafeExecutor2 == null
            || unsafeExecutor2.equals(EsExecutors.executorName(thread1));
    }
}
