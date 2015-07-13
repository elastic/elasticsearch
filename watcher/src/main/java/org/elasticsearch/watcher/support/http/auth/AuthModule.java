/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http.auth;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.watcher.support.http.auth.basic.ApplicableBasicAuth;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuth;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuthFactory;

/**
 */
public class AuthModule extends AbstractModule {

    @Override
    protected void configure() {

        MapBinder<String, HttpAuthFactory> parsersBinder = MapBinder.newMapBinder(binder(), String.class, HttpAuthFactory.class);

        bind(BasicAuthFactory.class).asEagerSingleton();
        parsersBinder.addBinding(BasicAuth.TYPE).to(BasicAuthFactory.class);

        bind(HttpAuthRegistry.class).asEagerSingleton();
    }
}
