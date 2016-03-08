/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.init;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.watcher.input.chain.ChainInputFactory;
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.transform.chain.ChainTransformFactory;

/**
 *
 */
public class InitializingModule extends AbstractModule {

    @Override
    protected void configure() {

        bind(WatcherClientProxy.class).asEagerSingleton();
        bind(ScriptServiceProxy.class).asEagerSingleton();
        bind(ChainInputFactory.class).asEagerSingleton();

        Multibinder<InitializingService.Initializable> mbinder = Multibinder.newSetBinder(binder(),
                InitializingService.Initializable.class);
        mbinder.addBinding().to(WatcherClientProxy.class);
        mbinder.addBinding().to(ScriptServiceProxy.class);
        mbinder.addBinding().to(ChainTransformFactory.class);
        mbinder.addBinding().to(ChainInputFactory.class);
        bind(InitializingService.class).asEagerSingleton();
    }
}
