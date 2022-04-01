/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.locator;

import org.elasticsearch.core.internal.provider.ProviderLocator;
import org.elasticsearch.logging.spi.AppenderSupport;
import org.elasticsearch.logging.spi.LogLevelSupport;
import org.elasticsearch.logging.spi.LogManagerFactory;
import org.elasticsearch.logging.spi.LoggingBootstrapSupport;
import org.elasticsearch.logging.spi.MessageFactory;
import org.elasticsearch.logging.spi.StringBuildersSupport;

import java.util.Collections;
import java.util.ServiceConfigurationError;
import java.util.Set;

public class LoggingSupportLocator {

    static final String PROVIDER_NAME = "logging";

    static final String PROVIDER_MODULE_NAME = "org.elasticsearch.logging.impl";

    static final Set<String> MISSING_MODULES = Collections.emptySet();

    static ProviderLocator providerLocator = new ProviderLocator(PROVIDER_NAME,  PROVIDER_MODULE_NAME);

    public static final LoggingBootstrapSupport LOGGING_BOOTSTRAP_SUPPORT_INSTANCE = getSupportInstance(LoggingBootstrapSupport.class);

    public static final LogLevelSupport LOG_LEVEL_SUPPORT_INSTANCE = getSupportInstance(LogLevelSupport.class);

    public static final MessageFactory MESSAGE_FACTORY_INSTANCE = getSupportInstance(MessageFactory.class);

    public static final AppenderSupport APPENDER_SUPPORT_INSTANCE = getSupportInstance(AppenderSupport.class);

    public static final StringBuildersSupport STRING_BUILDERS_SUPPORT_INSTANCE = getSupportInstance(StringBuildersSupport.class);

    public static final LogManagerFactory LOG_MANAGER_FACTORY_INSTANCE = getSupportInstance(LogManagerFactory.class);

@SuppressWarnings("unchecked")
    private static <T> T getSupportInstance(Class<T> supportClass) {
        Module m = LoggingSupportLocator.class.getModule();
        if (m.isNamed() && m.getDescriptor().uses().stream().anyMatch(supportClass.getName()::equals) == false) {
            throw new ServiceConfigurationError("%s: module %s does not declare `uses`".formatted(supportClass, m));
        }
        return providerLocator.get(supportClass);
    }
}
