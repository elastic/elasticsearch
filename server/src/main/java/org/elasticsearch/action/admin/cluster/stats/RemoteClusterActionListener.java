/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Action listener for operations that are performed on a group of remote clusters.
 * It will wait for all operations to complete and then delegate to the upstream listener.
 * Does not fail if one of the operations fails.
 * <br>
 * Returns a map of the results per cluster name via {@link #remoteListener(String)} method.
 * This is the listener that should be used to perform the individual operation on the remote cluster.
 *
 * @param <T> the type of the individual per-cluster result
 */
public class RemoteClusterActionListener<T> extends DelegatingActionListener<T, Map<String, T>> {
    private final CountDown countDown;
    private final Map<String, T> results;
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    public RemoteClusterActionListener(int groupSize, ActionListener<Map<String, T>> delegate) {
        super(delegate);
        if (groupSize <= 0) {
            assert false : "illegal group size [" + groupSize + "]";
            throw new IllegalArgumentException("groupSize must be greater than 0 but was " + groupSize);
        }
        results = new ConcurrentHashMap<>(groupSize);
        countDown = new CountDown(groupSize);
    }

    public ActionListener<T> remoteListener(String clusterAlias) {
        return delegateFailure((l, r) -> {
            results.put(clusterAlias, r);
            l.onResponse(r);
        });
    }

    @Override
    public void onResponse(T element) {
        if (countDown.countDown()) {
            delegate.onResponse(results);
        }
    }

    @Override
    public void onFailure(Exception e) {
        // TODO: how do we report the failures?
        final var firstException = failure.compareAndExchange(null, e);
        if (firstException != null && firstException != e) {
            firstException.addSuppressed(e);
        }
        if (countDown.countDown()) {
            delegate.onResponse(results);
        }
    }
}
