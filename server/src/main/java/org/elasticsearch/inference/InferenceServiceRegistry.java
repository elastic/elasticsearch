/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InferenceServiceRegistry implements Closeable {

    private final Map<String, InferenceService> services;
    private final List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

    public InferenceServiceRegistry(
        List<InferenceServiceExtension> inferenceServicePlugins,
        InferenceServiceExtension.InferenceServiceFactoryContext factoryContext
    ) {
        // TODO check names are unique
        services = inferenceServicePlugins.stream()
            .flatMap(r -> r.getInferenceServiceFactories().stream())
            .map(factory -> factory.create(factoryContext))
            .collect(Collectors.toMap(InferenceService::name, Function.identity()));
    }

    public void init(Client client) {
        services.values().forEach(s -> s.init(client));
    }

    public void onNodeStarted() {
        for (var service : services.values()) {
            try {
                service.onNodeStarted();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public Map<String, InferenceService> getServices() {
        return services;
    }

    public Optional<InferenceService> getService(String serviceName) {

        if ("elser".equals(serviceName)) { // ElserService.NAME before removal
            // here we are aliasing the elser service to use the elasticsearch service instead
            return Optional.ofNullable(services.get("elasticsearch")); // ElasticsearchInternalService.NAME
        } else {
            return Optional.ofNullable(services.get(serviceName));
        }
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return namedWriteables;
    }

    @Override
    public void close() throws IOException {
        for (var service : services.values()) {
            service.close();
        }
    }
}
