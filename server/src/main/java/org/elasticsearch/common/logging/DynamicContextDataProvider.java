/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.util.ContextDataProvider;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of log4j2's {@link ContextDataProvider} that can be configured at runtime
 * (after being loaded by log4j's init mechanism).
 *
 * A {@link ContextDataProvider} can enrich a logging event with additional "context data" (a string-to-string map of additional fields).
 * Elasticsearch (via {@link ECSJsonLayout} logs these additional fields in the json formatted log.
 *
 * Log4j loads {@link ContextDataProvider} instances using SPI during logging initialisation. In Elasticsearch this initialisation happens
 * early in the bootstrap process, before Elasticsearch modules and plugins are loaded. Thus, it is not possible for an Elasticsearch plugin
 * to register a {@link ContextDataProvider} service with log4j.
 *
 * Instead, Elasticsearch allows plugins to implement the {@link LoggingDataProvider} which is loaded via Elasticsearch's SPI implementation
 * (as part of plugin loading). This {@link DynamicContextDataProvider} class is the bridge between the Elasticsearch and log4j systems.
 */
public class DynamicContextDataProvider implements ContextDataProvider {

    // This is not a set-once because some integration tests may try to set it twice
    private static final AtomicReference<List<? extends LoggingDataProvider>> DATA_PROVIDERS = new AtomicReference<>();

    public static void setDataProviders(List<? extends LoggingDataProvider> dataProviders) {
        DynamicContextDataProvider.DATA_PROVIDERS.compareAndSet(null, List.copyOf(dataProviders));
    }

    @Override
    public Map<String, String> supplyContextData() {
        final List<? extends LoggingDataProvider> providers = DATA_PROVIDERS.get();
        if (providers != null && providers.isEmpty() == false) {
            final Map<String, String> data = new LinkedHashMap<>();
            providers.forEach(p -> p.collectData(data));
            return data;
        } else {
            return Map.of();
        }
    }
}
