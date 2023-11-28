/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class NodeServiceLocator {

    private final Map<Class<?>, ServiceReference<?>> serviceAccessors = new HashMap<>();
    private boolean accessible;

    private class ServiceReference<T> extends AtomicReference<T> {
        private final Class<T> cls;
        private T service;

        private ServiceReference(Class<T> cls) {
            this.cls = cls;
        }

        public T getService() {
            // fast path no-sync check
            if (service != null) {
                return service;
            }

            T service = get();
            if (accessible == false) throw new IllegalStateException("Service cannot be accessed here");
            if (service == null) throw new IllegalStateException("No instance of " + cls + " provided");
            return this.service = service;
        }
    }

    @SuppressWarnings("unchecked")
    public <T> Supplier<T> requestService(Class<T> service) {
        return (Supplier<T>) (Supplier<?>) serviceAccessors.computeIfAbsent(service, ServiceReference::new)::getService;
    }

    @SuppressWarnings("unchecked")
    <T> void setService(Class<T> cls, T service) {
        ((ServiceReference<Object>) serviceAccessors.computeIfAbsent(cls, ServiceReference::new)).set(service);
    }

    void setAccessible() {
        accessible = true;
    }
}
