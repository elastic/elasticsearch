/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;

import java.util.HashSet;
import java.util.Set;

public class ExporterModule extends AbstractModule {

    private final Set<Class<? extends Exporter>> exporters = new HashSet<>();

    public ExporterModule() {
        // Registers default exporter
        registerExporter(HttpESExporter.class);
    }

    @Override
    protected void configure() {
        Multibinder<Exporter> binder = Multibinder.newSetBinder(binder(), Exporter.class);
        for (Class<? extends Exporter> exporter : exporters) {
            bind(exporter).asEagerSingleton();
            binder.addBinding().to(exporter);
        }
    }

    public void registerExporter(Class<? extends Exporter> exporter) {
        exporters.add(exporter);
    }
}
