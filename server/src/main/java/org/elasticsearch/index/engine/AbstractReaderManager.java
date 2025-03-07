/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.search.ReferenceManager;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@SuppressForbidden(reason = "reference counting is required here")
public abstract class AbstractReaderManager extends ReferenceManager<ElasticsearchDirectoryReader> {

    @Nullable // if assertions are disabled
    private final Map<RefreshListener, AssertingRefreshListener> assertingListeners = Assertions.ENABLED ? new ConcurrentHashMap<>() : null;

    @Override
    protected boolean tryIncRef(ElasticsearchDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(ElasticsearchDirectoryReader reference) {
        return reference.getRefCount();
    }

    @Override
    protected void decRef(ElasticsearchDirectoryReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    public final void addListener(RefreshListener listener) {
        if (Assertions.ENABLED == false) {
            super.addListener(listener);
            return;
        }

        final var assertingListener = new AssertingRefreshListener(listener);
        var previous = assertingListeners.put(listener, assertingListener);
        assert previous == null : "listener already added";
        super.addListener(assertingListener);
    }

    @Override
    public final void removeListener(RefreshListener listener) {
        if (Assertions.ENABLED == false) {
            super.removeListener(listener);
            return;
        }

        final var assertingListener = assertingListeners.remove(listener);
        assert assertingListener != null : "listener already removed";
        super.removeListener(assertingListener);
    }

    /**
     * A delegating {@link RefreshListener} used to assert that refresh listeners are not accessing the engine within before/after refresh
     * methods.
     */
    private static class AssertingRefreshListener implements RefreshListener {

        private final RefreshListener delegate;

        private AssertingRefreshListener(RefreshListener delegate) {
            this.delegate = Objects.requireNonNull(delegate);
            if (Assertions.ENABLED == false) {
                throw new AssertionError("Only use this when assertions are enabled");
            }
        }

        @Override
        public void beforeRefresh() throws IOException {
            SafeEngineAccessThreadLocal.accessStart();
            try {
                delegate.beforeRefresh();
            } finally {
                SafeEngineAccessThreadLocal.accessEnd();
            }
        }

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            SafeEngineAccessThreadLocal.accessStart();
            try {
                delegate.afterRefresh(didRefresh);
            } finally {
                SafeEngineAccessThreadLocal.accessEnd();
            }
        }
    }
}
