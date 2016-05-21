/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.Monitoring;
import org.elasticsearch.marvel.agent.exporter.http.HttpExporter;
import org.elasticsearch.marvel.agent.exporter.local.LocalExporter;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.HashMap;
import java.util.Map;

public class ExporterModule extends AbstractModule {

    private final Settings settings;
    private final Map<String, Class<? extends Exporter.Factory<? extends Exporter>>> exporterFactories = new HashMap<>();

    public ExporterModule(Settings settings) {
        this.settings = settings;
        registerExporter(HttpExporter.TYPE, HttpExporter.Factory.class);
        registerExporter(LocalExporter.TYPE, LocalExporter.Factory.class);
    }

    @Override
    protected void configure() {
        if (Monitoring.enabled(settings) && XPackPlugin.transportClientMode(settings) == false
                && XPackPlugin.isTribeNode(settings) == false) {
            bind(Exporters.class).asEagerSingleton();
            MapBinder<String, Exporter.Factory> factoryBinder = MapBinder.newMapBinder(binder(), String.class, Exporter.Factory.class);
            for (Map.Entry<String, Class<? extends Exporter.Factory<? extends Exporter>>> entry : exporterFactories.entrySet()) {
                bind(entry.getValue()).asEagerSingleton();
                factoryBinder.addBinding(entry.getKey()).to(entry.getValue());
            }
        } else {
            bind(Exporters.class).toProvider(Providers.of(null));
        }
    }

    public void registerExporter(String type, Class<? extends Exporter.Factory<? extends Exporter>> factory) {
        exporterFactories.put(type, factory);
    }
}
