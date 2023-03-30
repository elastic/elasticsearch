/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.reservedstate.ReservedClusterStateHandlerProvider;

/**
 * Mock Security Provider implementation for the {@link ReservedClusterStateHandlerProvider} service interface. This is used
 * for {@link org.elasticsearch.test.ESIntegTestCase} because the Security Plugin is really LocalStateSecurity in those tests.
 * <p>
 * Unlike {@link LocalReservedSecurityStateHandlerProvider} this implementation is mocked to implement the
 * {@link UnstableLocalStateSecurity}. Separate implementation is needed, because the SPI creation code matches the constructor
 * signature when instantiating. E.g. we need to match {@link UnstableLocalStateSecurity} instead of {@link LocalStateSecurity}
 */
public class LocalReservedUnstableSecurityStateHandlerProvider extends LocalReservedSecurityStateHandlerProvider {
    public LocalReservedUnstableSecurityStateHandlerProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public LocalReservedUnstableSecurityStateHandlerProvider(UnstableLocalStateSecurity plugin) {
        super(plugin);
    }
}
