/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.marvel.agent.exporter.local.LocalExporter;
import org.elasticsearch.marvel.shield.MarvelSettingsFilter;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.*;

/**
 *
 */
public class Exporters extends AbstractLifecycleComponent<Exporters> implements Iterable<Exporter>, NodeSettingsService.Listener {

    static final String EXPORTERS_SETTING = "marvel.agent.exporters";

    private final Map<String, Exporter.Factory> factories;
    private final MarvelSettingsFilter settingsFilter;
    private final ClusterService clusterService;

    private volatile InternalExporters exporters = InternalExporters.EMPTY;

    @Inject
    public Exporters(Settings settings, Map<String, Exporter.Factory> factories,
                     MarvelSettingsFilter settingsFilter, ClusterService clusterService,
                     NodeSettingsService nodeSettingsService) {

        super(settings);
        this.factories = factories;
        this.settingsFilter = settingsFilter;
        this.clusterService = clusterService;
        nodeSettingsService.addListener(this);
    }

    @Override
    protected void doStart() {
        exporters = initExporters(settings.getAsSettings(EXPORTERS_SETTING));
    }

    @Override
    protected void doStop() {
        ElasticsearchException exception = null;
        for (Exporter exporter : exporters) {
            try {
                exporter.close();
            } catch (Exception e) {
                logger.error("exporter [{}] failed to close cleanly", e, exporter.name());
                if (exception == null) {
                    exception = new ElasticsearchException("failed to cleanly close exporters");
                }
                exception.addSuppressed(e);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    protected void doClose() {
    }

    public Exporter getExporter(String name) {
        return exporters.get(name);
    }

    @Override
    public Iterator<Exporter> iterator() {
        return exporters.iterator();
    }

    public void export(Collection<MarvelDoc> marvelDocs) {
        for (Exporter exporter : exporters) {
            if (exporter.masterOnly() && !clusterService.localNode().masterNode()) {
                // the exporter is supposed to only run on the master node, but we're not
                // the master node... so skipping
                continue;
            }
            try {
                exporter.export(marvelDocs);
            } catch (Exception e) {
                logger.error("exporter [{}] failed to export marvel data", e, exporter.name());
            }
        }
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        InternalExporters existing = exporters;
        Settings updatedSettings = settings.getAsSettings(EXPORTERS_SETTING);
        if (updatedSettings.names().isEmpty()) {
            return;
        }
        this.exporters = initExporters(Settings.builder()
                .put(existing.settings)
                .put(updatedSettings)
                .build());
        existing.close(logger);
    }

    InternalExporters initExporters(Settings settings) {
        Set<String> singletons = new HashSet<>();
        Map<String, Exporter> exporters = new HashMap<>();
        boolean hasDisabled = false;
        for (String name : settings.names()) {
            Settings exporterSettings = settings.getAsSettings(name);
            String type = exporterSettings.get("type");
            if (type == null) {
                throw new SettingsException("missing exporter type for [" + name + "] exporter");
            }
            Exporter.Factory factory = factories.get(type);
            if (factory == null) {
                throw new SettingsException("unknown exporter type [" + type + "] set for exporter [" + name + "]");
            }
            factory.filterOutSensitiveSettings(EXPORTERS_SETTING + ".*.", settingsFilter);
            Exporter.Config config = new Exporter.Config(name, settings, exporterSettings);
            if (!config.enabled()) {
                hasDisabled = true;
                if (logger.isDebugEnabled()) {
                    logger.debug("exporter [{}/{}] is disabled", type, name);
                }
                continue;
            }
            if (factory.singleton()) {
                // this is a singleton exporter factory, let's make sure we didn't already registered one
                // (there can only be one instance of a singleton exporter)
                if (singletons.contains(type)) {
                    throw new SettingsException("multiple [" + type + "] exporters are configured. there can " +
                            "only be one [" + type + "] exporter configured");
                }
                singletons.add(type);
            }
            exporters.put(config.name(), factory.create(config));
        }

        // no exporters are configured, lets create a default local one.
        //
        // NOTE:    if there are exporters configured and they're all disabled, we don't
        //          fallback on the default
        //
        if (exporters.isEmpty() && !hasDisabled) {
            Exporter.Config config = new Exporter.Config("default_" + LocalExporter.TYPE, settings, Settings.EMPTY);
            exporters.put(config.name(), factories.get(LocalExporter.TYPE).create(config));
        }

        return new InternalExporters(settings, exporters);
    }

    public static void registerDynamicSettings(ClusterModule clusterModule) {
        clusterModule.registerClusterDynamicSetting(EXPORTERS_SETTING + "*", Validator.EMPTY);
    }

    static class InternalExporters implements Iterable<Exporter> {

        static final InternalExporters EMPTY = new InternalExporters(Settings.EMPTY, Collections.emptyMap());

        final Settings settings;
        final Map<String, Exporter> exporters;

        public InternalExporters(Settings settings, Map<String, Exporter> exporters) {
            this.settings = settings;
            this.exporters = exporters;
        }

        @Override
        public Iterator<Exporter> iterator() {
            return exporters.values().iterator();
        }

        public Exporter get(String name) {
            return exporters.get(name);
        }

        void close(ESLogger logger) {
            for (Exporter exporter : exporters.values()) {
                try {
                    exporter.close();
                } catch (Exception e) {
                    logger.error("failed to close exporter [{}]", e, exporter.name());
                }
            }
        }
    }
}
