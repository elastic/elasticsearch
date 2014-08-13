/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.SecurityFilter;

/**
 *
 */
public class SecuredTransportModule extends AbstractModule {

    private final Settings settings;

    public SecuredTransportModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        if (!settings.getAsBoolean("node.client", false)) {
            bind(TransportFilter.class).to(SecurityFilter.Transport.class).asEagerSingleton();
        }
    }
}
