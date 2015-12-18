/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.marvel.agent.exporter.local.LocalExporter;
import org.elasticsearch.marvel.shield.MarvelSettingsFilter;

import java.util.*;

/**
 *
 */
public class Exporters extends AbstractLifecycleComponent<Exporters> implements Iterable<Exporter> {

    public static final Setting<Settings> EXPORTERS_SETTING = Setting.groupSetting("marvel.agent.exporters.", true, Setting.Scope.CLUSTER);

    private final Map<String, Exporter.Factory> factories;
    private final MarvelSettingsFilter settingsFilter;
    private final ClusterService clusterService;

    private volatile CurrentExporters exporters = CurrentExporters.EMPTY;
    private volatile Settings exporterSettings;

    @Inject
    public Exporters(Settings settings, Map<String, Exporter.Factory> factories,
                     MarvelSettingsFilter settingsFilter, ClusterService clusterService,
                     ClusterSettings clusterSettings) {

        super(settings);
        this.factories = factories;
        this.settingsFilter = settingsFilter;
        this.clusterService = clusterService;
        exporterSettings = EXPORTERS_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(EXPORTERS_SETTING, this::setExportersSetting);

    }

    private synchronized void setExportersSetting(Settings exportersSetting) {
        this.exporterSettings = exportersSetting;
        if (this.lifecycleState() == Lifecycle.State.STARTED) {

            CurrentExporters existing = exporters;
            Settings updatedSettings = exportersSetting;
            if (updatedSettings.names().isEmpty()) {
                return;
            }
            this.exporters = initExporters(Settings.builder()
                    .put(existing.settings)
                    .put(updatedSettings)
                    .build());
            existing.close(logger);
        }
    }

    @Override
    protected void doStart() {
        synchronized (this) {
            exporters = initExporters(exporterSettings);
        }
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

    public ExportBulk openBulk() {
        List<ExportBulk> bulks = new ArrayList<>();
        for (Exporter exporter : exporters) {
            if (exporter.masterOnly() && !clusterService.localNode().masterNode()) {
                // the exporter is supposed to only run on the master node, but we're not
                // the master node... so skipping
                continue;
            }
            try {
                ExportBulk bulk = exporter.openBulk();
                if (bulk == null) {
                    logger.info("skipping exporter [{}] as it isn't ready yet", exporter.name());
                } else {
                    bulks.add(bulk);
                }
            } catch (Exception e) {
                logger.error("exporter [{}] failed to export marvel data", e, exporter.name());
            }
        }
        return bulks.isEmpty() ? null : new ExportBulk.Compound(bulks);
    }

    // TODO only rebuild the exporters that need to be updated according to settings
    CurrentExporters initExporters(Settings settings) {
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

        return new CurrentExporters(settings, exporters);
    }

    static class CurrentExporters implements Iterable<Exporter> {

        static final CurrentExporters EMPTY = new CurrentExporters(Settings.EMPTY, Collections.emptyMap());

        final Settings settings;
        final Map<String, Exporter> exporters;

        public CurrentExporters(Settings settings, Map<String, Exporter> exporters) {
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
