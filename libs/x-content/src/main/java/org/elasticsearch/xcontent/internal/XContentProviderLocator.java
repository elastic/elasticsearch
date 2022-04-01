/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import org.elasticsearch.core.internal.provider.ProviderLocator;
import org.elasticsearch.xcontent.spi.XContentProvider;

import java.io.IOException;
import java.util.ServiceConfigurationError;
import java.util.Set;

/**
 * A provider locator for finding the {@link XContentProvider}.
 */
public final class XContentProviderLocator {

    static final String PROVIDER_NAME = "x-content";

    static final String PROVIDER_MODULE_NAME = "org.elasticsearch.xcontent.impl";

    static final Set<String> MISSING_MODULES = Set.of("com.fasterxml.jackson.databind");

    /**
     * Returns the provider instance.
     */
    public static final XContentProvider INSTANCE = provider();

    @SuppressWarnings("unchecked")
    private static XContentProvider provider() {
        Module m = XContentProviderLocator.class.getModule();
        if (m.isNamed() && m.getDescriptor().uses().stream().anyMatch(XContentProvider.class.getName()::equals) == false) {
            throw new ServiceConfigurationError("%s: module %s does not declare `uses`".formatted(XContentProvider.class, m));
        }
        ProviderLocator providerLocator =  new ProviderLocator(PROVIDER_NAME, PROVIDER_MODULE_NAME, MISSING_MODULES);

        return  providerLocator.get(XContentProvider.class);
    }
}
