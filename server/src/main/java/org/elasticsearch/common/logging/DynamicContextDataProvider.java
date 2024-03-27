/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.util.ContextDataProvider;
import org.apache.lucene.util.SetOnce;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DynamicContextDataProvider implements ContextDataProvider {

    private static final SetOnce<List<? extends LoggingDataProvider>> DATA_PROVIDERS = new SetOnce<>();

    public static void setDataProviders(List<? extends LoggingDataProvider> dataProviders) {
        DynamicContextDataProvider.DATA_PROVIDERS.set(List.copyOf(dataProviders));
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
