/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public class SSLServiceProvider implements Provider<SSLService> {

    private final Settings settings;

    private SSLService service;

    @Inject
    public SSLServiceProvider(Settings settings) {
        this.settings = settings;
    }

    @Override
    public synchronized SSLService get() {
        if (service == null) {
            service = new SSLService(settings);
        }
        return service;
    }
}
