/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.init;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;

import java.util.HashSet;
import java.util.Set;

/**
 * A module to lazy initialize objects and avoid circular dependency injection issues.
 *
 * Objects that use the {@link org.elasticsearch.client.ElasticsearchClient} and that are also injected in transport actions provoke
 * a circular dependency injection issues with Guice. Using proxies with lazy initialization is a way to solve this issue.
 *
 * The proxies are initialized by {@link LazyInitializationService}.
 */
public class LazyInitializationModule extends AbstractModule {

    private final Set<Class<? extends LazyInitializable>> initializables = new HashSet<>();

    @Override
    protected void configure() {
        Multibinder<LazyInitializable> mbinder = Multibinder.newSetBinder(binder(), LazyInitializable.class);
        for (Class<? extends LazyInitializable> initializable : initializables) {
            bind(initializable).asEagerSingleton();
            mbinder.addBinding().to(initializable);
        }
        bind(LazyInitializationService.class).asEagerSingleton();
    }

    public void registerLazyInitializable(Class<? extends LazyInitializable> lazyTypeClass) {
        initializables.add(lazyTypeClass);
    }
}
