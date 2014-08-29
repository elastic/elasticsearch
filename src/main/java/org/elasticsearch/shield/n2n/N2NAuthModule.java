/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.n2n;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public class N2NAuthModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(IPFilteringN2NAuthenticator.class).asEagerSingleton();
        bind(N2NNettyUpstreamHandler.class).asEagerSingleton();
    }
}
