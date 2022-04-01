/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.locator;

import org.elasticsearch.core.internal.provider.ProviderLocator;
import org.elasticsearch.logging.spi.LoggingSupportProvider;

import java.util.Collections;
import java.util.ServiceConfigurationError;
import java.util.Set;

public class LoggingSupportLocator {

    static final String PROVIDER_NAME = "logging";

    static final String PROVIDER_MODULE_NAME = "org.elasticsearch.logging.impl";

    static final Set<String> MISSING_MODULES = Collections.emptySet();

    public static final LoggingSupportProvider LOGGING_SUPPORT_INSTANCE = getSupportInstance();

    @SuppressWarnings("unchecked")
    private static LoggingSupportProvider getSupportInstance() {
        Module m = LoggingSupportLocator.class.getModule();
        if (m.isNamed() && m.getDescriptor().uses().stream().anyMatch(LoggingSupportProvider.class.getName()::equals) == false) {
            throw new ServiceConfigurationError("%s: module %s does not declare `uses`".formatted(LoggingSupportProvider.class, m));
        }

        return (new ProviderLocator<>(PROVIDER_NAME, LoggingSupportProvider.class, PROVIDER_MODULE_NAME, MISSING_MODULES)).get();
    }
}
