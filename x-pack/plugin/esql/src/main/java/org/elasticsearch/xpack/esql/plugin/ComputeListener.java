/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.compute.EsqlRefCountingListener;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.ResponseHeadersCollector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A variant of {@link RefCountingListener} with the following differences:
 * 1. Automatically cancels sub tasks on failure (via runOnTaskFailure)
 * 2. Collects driver profiles from sub tasks.
 * 3. Collects response headers from sub tasks, specifically warnings emitted during compute
 * 4. Collects failures and returns the most appropriate exception to the caller.
 */
final class ComputeListener implements Releasable {
    private final EsqlRefCountingListener refs;
    private final List<DriverProfile> collectedProfiles;
    private final ResponseHeadersCollector responseHeaders;
    private final Runnable runOnFailure;

    ComputeListener(ThreadPool threadPool, Runnable runOnFailure, ActionListener<List<DriverProfile>> delegate) {
        this.runOnFailure = runOnFailure;
        this.responseHeaders = new ResponseHeadersCollector(threadPool.getThreadContext());
        this.collectedProfiles = Collections.synchronizedList(new ArrayList<>());
        // listener that executes after all the sub-listeners refs (created via acquireCompute) have completed
        this.refs = new EsqlRefCountingListener(delegate.delegateFailure((l, ignored) -> {
            responseHeaders.finish();
            delegate.onResponse(collectedProfiles.stream().toList());
        }));
    }

    /**
     * Acquires a new listener that doesn't collect result
     */
    ActionListener<Void> acquireAvoid() {
        var listener = ActionListener.assertAtLeastOnce(refs.acquire());
        return listener.delegateResponse((l, e) -> {
            try {
                runOnFailure.run();
            } finally {
                l.onFailure(e);
            }
        });
    }

    /**
     * Acquires a new listener that collects compute result. This listener will also collect warnings emitted during compute
     */
    ActionListener<List<DriverProfile>> acquireCompute() {
        final ActionListener<Void> delegate = acquireAvoid();
        return ActionListener.wrap(profiles -> {
            responseHeaders.collect();
            if (profiles != null && profiles.isEmpty() == false) {
                collectedProfiles.addAll(profiles);
            }
            delegate.onResponse(null);
        }, e -> {
            responseHeaders.collect();
            delegate.onFailure(e);
        });
    }

    @Override
    public void close() {
        refs.close();
    }
}
