/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner.spi;
import org.elasticsearch.core.internal.provider.ProviderLocator;

import java.util.Collections;
import java.util.ServiceConfigurationError;
import java.util.Set;
public class StablePluginRegistryLocator {
    static final String PROVIDER_NAME = "plugin-scanner";

    static final String PROVIDER_MODULE_NAME = "org.elasticsearch.plugin.scanner.impl";

    static final Set<String> MISSING_MODULES = Collections.emptySet();

    public static final StablePluginRegistryProvider STABLE_PLUGIN_REGISTRY_PROVIDER = getInstance();

    @SuppressWarnings("unchecked")
    private static StablePluginRegistryProvider getInstance() {
        Module m = StablePluginRegistryLocator.class.getModule();
        if (m.isNamed() && m.getDescriptor().uses().stream().anyMatch(StablePluginRegistryProvider.class.getName()::equals) == false) {
            throw new ServiceConfigurationError("%s: module %s does not declare `uses`".formatted(StablePluginRegistryProvider.class, m));
        }

        return (new ProviderLocator<>(PROVIDER_NAME, StablePluginRegistryProvider.class, PROVIDER_MODULE_NAME, MISSING_MODULES)).get();
    }
}
