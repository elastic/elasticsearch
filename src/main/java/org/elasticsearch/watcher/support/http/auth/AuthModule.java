/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http.auth;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

/**
 */
public class AuthModule extends AbstractModule {

    @Override
    protected void configure() {
        MapBinder<String, HttpAuth.Parser> parsersBinder = MapBinder.newMapBinder(binder(), String.class, HttpAuth.Parser.class);
        bind(BasicAuth.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(BasicAuth.TYPE).to(BasicAuth.Parser.class);
        bind(HttpAuthRegistry.class).asEagerSingleton();
    }
}
