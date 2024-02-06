/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.util.ContextDataProvider;
import org.apache.logging.log4j.util.StringMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LoggingContextProvider implements ContextDataProvider {

    // TODO Should be thread safe for testing purposes
    private static final List<Supplier<Map<String, String>>> loggingContextProviders = new ArrayList<>();

    public static void registerLoggingContextProvider(Supplier<Map<String, String>> logginContextSupplier) {
        loggingContextProviders.add(logginContextSupplier);
    }

    @Override
    public Map<String, String> supplyContextData() {
        return loggingContextProviders.stream()
            .flatMap(supplier -> supplier.get().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public StringMap supplyStringMap() {
        return ContextDataProvider.super.supplyStringMap();
    }
}
