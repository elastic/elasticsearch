/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action;

import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.action.filter.ShieldActionFilter;
import org.elasticsearch.shield.action.interceptor.BulkRequestInterceptor;
import org.elasticsearch.shield.action.interceptor.RealtimeRequestInterceptor;
import org.elasticsearch.shield.action.interceptor.RequestInterceptor;
import org.elasticsearch.shield.action.interceptor.SearchRequestInterceptor;
import org.elasticsearch.shield.action.interceptor.UpdateRequestInterceptor;
import org.elasticsearch.shield.support.AbstractShieldModule;

public class ShieldActionModule extends AbstractShieldModule.Node {

    public ShieldActionModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        bind(ShieldActionMapper.class).asEagerSingleton();
        // we need to ensure that there's only a single instance of the action filters
        bind(ShieldActionFilter.class).asEagerSingleton();

        // TODO: we should move these to action filters and only have 1 chain.
        Multibinder<RequestInterceptor> multibinder
                = Multibinder.newSetBinder(binder(), RequestInterceptor.class);
        multibinder.addBinding().to(RealtimeRequestInterceptor.class);
        multibinder.addBinding().to(SearchRequestInterceptor.class);
        multibinder.addBinding().to(UpdateRequestInterceptor.class);
        multibinder.addBinding().to(BulkRequestInterceptor.class);
    }
}
