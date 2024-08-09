/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.injector;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nalbind.exceptions.InjectionConfigurationException;
import org.elasticsearch.nalbind.exceptions.UnresolvedProxyException;

import java.util.AbstractList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Proxies generally have a lifetime that is not bound to any one injection plan,
 * and objects from multiple injectors/plans must be grouped into the same list.
 */
class ProxyPool {
    private final Map<Class<?>, List<?>> proxyInstances = new LinkedHashMap<>();
    private final Map<Class<?>, ProxyPool.ProxyInfo<? extends List<?>>> unresolvedListProxies = new LinkedHashMap<>();

    private record ProxyInfo<T>(Class<T> interfaceType, T proxyObject, Consumer<T> setter) {}

    List<?> theProxyFor(Class<?> elementType) {
        List<?> result = proxyInstances.get(elementType);
        if (result == null) {
            throw new InjectionConfigurationException("No proxy for " + elementType.getSimpleName());
        } else {
            return result;
        }
    }

    /**
     * @return the existing proxy, or null if none
     */
    public <T> List<T> putNewListProxy(Class<T> elementType) {
        LOGGER.trace("Creating list proxy for {}", elementType.getSimpleName());
        ProxyPool.ProxyInfo<List<T>> proxyInfo = generateListProxyFor(elementType);
        unresolvedListProxies.put(elementType, proxyInfo);
        return proxyInfo.interfaceType().cast(proxyInstances.put(elementType, proxyInfo.proxyObject()));
    }

    /**
     * <em>Evolution note</em>: when we have multiple domains/subsystems, we'll actually need to resolve
     * proxies to contain all instances from all subsystems. But that's a tomorrow problem.
     */
    public void resolveListProxy(Class<?> elementType, List<Object> instances) {
        resolveListProxy(elementType, instances, requireNonNull(unresolvedListProxies.remove(elementType)));
    }

    /**
     * Extracting this method helps us convince the Java type system that are generics are kosher.
     */
    private static <L extends List<?>> void resolveListProxy(Class<?> elementType, List<Object> instances, ProxyInfo<L> p) {
        L List = p.interfaceType().cast(instances);
        LOGGER.trace("- {}: {} instances", elementType.getSimpleName(), List.size());
        p.setter().accept(List);
    }

    private <T> ProxyInfo<List<T>> generateListProxyFor(Class<T> elementType) {
        AtomicReference<List<T>> delegate = new AtomicReference<>(null);
        return new ProxyInfo<>(listClass(), new ListProxy<>(delegate, elementType), delegate::set);
    }

    // TODO: A more performant proxy
    private static final class ListProxy<T> extends AbstractList<T> {

        private final AtomicReference<List<T>> delegate;
        private final Class<T> elementType;

        ListProxy(AtomicReference<List<T>> delegate, Class<T> elementType) {
            this.delegate = delegate;
            this.elementType = elementType;
        }

        @Override
        public T get(int index) {
            return delegate().get(index);
        }

        @Override
        public int size() {
            return delegate().size();
        }

        private List<T> delegate() {
            List<T> result = delegate.get();
            if (result == null) {
                throw new UnresolvedProxyException(
                    "Missing @Actual annotation; cannot call method on injected List during object construction. " +
                        "Element type is " + elementType);
            } else {
                return result;
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <T> Class<List<T>> listClass() {
        return (Class<List<T>>)(Class)List.class;
    }

    private static final Logger LOGGER = LogManager.getLogger(ProxyInfo.class);
}
