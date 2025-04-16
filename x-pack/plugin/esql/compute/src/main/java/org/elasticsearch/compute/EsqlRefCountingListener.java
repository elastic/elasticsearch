/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.core.Releasable;

/**
 * Similar to {@link org.elasticsearch.action.support.RefCountingListener},
 * but prefers non-task-cancelled exceptions over task-cancelled ones as they are more useful for diagnosing issues.
 * @see FailureCollector
 */
public final class EsqlRefCountingListener implements Releasable {
    private final FailureCollector failureCollector;
    private final RefCountingRunnable refs;

    public EsqlRefCountingListener(ActionListener<Void> delegate) {
        this.failureCollector = new FailureCollector();
        this.refs = new RefCountingRunnable(() -> {
            Exception error = failureCollector.getFailure();
            if (error != null) {
                delegate.onFailure(error);
            } else {
                delegate.onResponse(null);
            }
        });
    }

    public ActionListener<Void> acquire() {
        var listener = ActionListener.assertAtLeastOnce(refs.acquireListener());
        return listener.delegateResponse((l, e) -> {
            failureCollector.unwrapAndCollect(e);
            l.onFailure(e);
        });
    }

    @Override
    public void close() {
        refs.close();
    }
}
