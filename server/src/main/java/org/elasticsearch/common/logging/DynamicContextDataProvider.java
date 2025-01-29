/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.util.ContextDataProvider;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.plugins.internal.LoggingDataProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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

    /**
     * Use to track the largest map size we've produced so that we can pre-allocate the next map to be the same size and reduce
     * reallocation costs
     */
    private final AtomicInteger mapSize = new AtomicInteger(0);

    public static void setDataProviders(List<? extends LoggingDataProvider> dataProviders) {
        DynamicContextDataProvider.DATA_PROVIDERS.compareAndSet(null, List.copyOf(dataProviders));
    }

    @Override
    public Map<String, String> supplyContextData() {
        final List<? extends LoggingDataProvider> providers = DATA_PROVIDERS.get();
        if (providers != null && providers.isEmpty() == false) {
            var expectedSize = mapSize.get();
            if (expectedSize == 0) {
                // This is the first map we've produced, so start with an allocation that ought to be big enough, but not too big
                expectedSize = 10;
            }
            final Map<String, String> data = Maps.newLinkedHashMapWithExpectedSize(expectedSize);
            providers.forEach(p -> p.collectData(data));
            final var newMapSize = data.size();
            mapSize.updateAndGet(oldSize -> oldSize >= newMapSize ? oldSize : newMapSize);
            return data;
        } else {
            return Map.of();
        }
    }
}
