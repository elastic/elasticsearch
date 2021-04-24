/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ConcurrentHashMap;

@FunctionalInterface
public interface AsyncSupplier<V> {

    /**
     * Asynchronously retrieves the value that is being supplied and notifies the listener upon
     * completion.
     */
    void getAsync(ActionListener<V> listener);

    class CachingAsyncSupplier<V> implements AsyncSupplier<V> {

        private final AsyncSupplier<V> asyncSupplierDelegate;
        private final ConcurrentHashMap<AsyncSupplier<V>, ListenableFuture<V>> map;

        public CachingAsyncSupplier(AsyncSupplier<V> supplierDelegate) {
            this.asyncSupplierDelegate = supplierDelegate;
            this.map = new ConcurrentHashMap<>(1);
        }

        @Override
        public void getAsync(ActionListener<V> listener) {
            map.computeIfAbsent(asyncSupplierDelegate, ignore -> {
                ListenableFuture<V> cachedListener = new ListenableFuture();
                // trigger async computation
                asyncSupplierDelegate.getAsync(cachedListener);
                return cachedListener;
            }).addListener(listener, EsExecutors.newDirectExecutorService());
        }
    }
}
