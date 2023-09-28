/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.InferenceServicePlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InferenceServiceRegistry extends AbstractLifecycleComponent {

    private final Map<String, InferenceService> services;
    private final List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

    public InferenceServiceRegistry(
        List<InferenceServicePlugin> inferenceServicePlugins,
        InferenceServicePlugin.InferenceServiceFactoryContext factoryContext
    ) {
        // TODO check names are unique
        services = inferenceServicePlugins.stream()
            .flatMap(r -> r.getInferenceServiceFactories().stream())
            .map(factory -> factory.create(factoryContext))
            .collect(Collectors.toMap(InferenceService::name, Function.identity()));

        for (var plugin : inferenceServicePlugins) {
            namedWriteables.addAll(plugin.getInferenceServiceNamedWriteables());
        }
    }

    public Map<String, InferenceService> getServices() {
        return services;
    }

    public Optional<InferenceService> getService(String serviceName) {
        return Optional.ofNullable(services.get(serviceName));
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return namedWriteables;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }
}
